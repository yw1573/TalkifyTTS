package com.github.lonepheasantwarrior.talkify.service.engine.impl

import android.speech.tts.Voice
import com.github.lonepheasantwarrior.talkify.R
import com.github.lonepheasantwarrior.talkify.TalkifyAppHolder
import com.github.lonepheasantwarrior.talkify.infrastructure.xml.VoiceXmlParser
import com.github.lonepheasantwarrior.talkify.domain.model.BaseEngineConfig
import com.github.lonepheasantwarrior.talkify.domain.model.MiniMaxTtsConfig
import com.github.lonepheasantwarrior.talkify.service.TtsErrorCode
import com.github.lonepheasantwarrior.talkify.service.TtsLogger
import com.github.lonepheasantwarrior.talkify.service.engine.AbstractTtsEngine
import com.github.lonepheasantwarrior.talkify.service.engine.AudioConfig
import com.github.lonepheasantwarrior.talkify.service.engine.SynthesisParams
import com.github.lonepheasantwarrior.talkify.service.engine.TtsSynthesisListener
import javazoom.jl.decoder.Bitstream
import javazoom.jl.decoder.Decoder
import javazoom.jl.decoder.Header
import javazoom.jl.decoder.SampleBuffer
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.withContext
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import org.json.JSONObject
import java.io.PipedInputStream
import java.io.PipedOutputStream
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.Locale
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.math.roundToInt

/**
 * MiniMax - 语音合成引擎实现（WebSocket 版）
 *
 * 继承 [AbstractTtsEngine]，实现 TTS 引擎接口
 * 基于 OkHttp WebSocket 实现流式音频合成，相比 HTTP 方案显著降低首字播放延迟
 *
 * 引擎 ID：minimax-tts
 * 服务提供商：MiniMax
 * API 文档：https://platform.minimaxi.com/docs/llms.txt
 */
class MiniMaxTtsEngine : AbstractTtsEngine() {

    companion object {
        const val ENGINE_ID = "minimax-tts"
        const val ENGINE_NAME = "MiniMax语音合成"
        private const val VOICE_NAME_SEPARATOR = "::"
        private const val WSS_URL = "wss://api.minimaxi.com/ws/v1/t2a_v2"
        private const val DEFAULT_MODEL = "speech-2.8-hd"

        private const val MAX_TEXT_LENGTH = 10000

        private val SUPPORTED_LANGUAGES = arrayOf("zho", "eng")

        private const val PIPE_BUFFER_SIZE = 65536
    }

    private val engineJob = SupervisorJob()
    private val engineScope = CoroutineScope(Dispatchers.IO + engineJob)

    private val client = OkHttpClient.Builder()
        .connectTimeout(10, TimeUnit.SECONDS)
        .readTimeout(0, TimeUnit.MILLISECONDS)
        .writeTimeout(30, TimeUnit.SECONDS)
        .pingInterval(30, TimeUnit.SECONDS)
        .build()

    @Volatile
    private var isCancelled = false

    @Volatile
    private var hasCompleted = false

    @Volatile
    private var currentWebSocket: WebSocket? = null

    private var synthesisJob: Job? = null

    /**
     * 缓存的声音ID列表，从资源文件加载
     */
    private val voiceIds: List<String> by lazy {
        loadVoiceIdsFromResource()
    }

    val audioConfig: AudioConfig
        @JvmName("getAudioConfigProperty") get() = AudioConfig.MINI_MAX_TTS

    private fun loadVoiceIdsFromResource(): List<String> {
        val context = TalkifyAppHolder.getContext()
        return if (context != null) {
            try {
                VoiceXmlParser.parseVoiceIds(context, R.xml.minimax_voices)
            } catch (e: Exception) {
                TtsLogger.e("Failed to load voice IDs from resource", throwable = e)
                emptyList()
            }
        } else {
            TtsLogger.w("Context not available, voice IDs will be empty")
            emptyList()
        }
    }

    override fun getEngineId(): String = ENGINE_ID

    override fun getEngineName(): String = ENGINE_NAME

    override fun getAudioConfig(): AudioConfig = audioConfig

    override fun synthesize(
        text: String,
        params: SynthesisParams,
        config: BaseEngineConfig,
        listener: TtsSynthesisListener
    ) {
        checkNotReleased()

        val miniMaxConfig = config as? MiniMaxTtsConfig
        if (miniMaxConfig == null) {
            logError("Invalid config type, expected MiniMaxTtsConfig")
            listener.onError(TtsErrorCode.getErrorMessage(TtsErrorCode.ERROR_ENGINE_NOT_CONFIGURED))
            return
        }

        if (miniMaxConfig.apiKey.isEmpty()) {
            logError("API Key is not configured")
            listener.onError(TtsErrorCode.getErrorMessage(TtsErrorCode.ERROR_ENGINE_NOT_CONFIGURED))
            return
        }

        if (text.isEmpty()) {
            logWarning("待朗读文本内容为空")
            listener.onSynthesisCompleted()
            return
        }

        if (!containsReadableText(text)) {
            logWarning("文本不包含可朗读的文字内容")
            listener.onSynthesisCompleted()
            return
        }

        logInfo("Starting synthesis: textLength=${text.length}, pitch=${params.pitch}, speechRate=${params.speechRate}")

        isCancelled = false
        hasCompleted = false

        synthesisJob?.cancel()
        synthesisJob = engineScope.launch {
            try {
                listener.onSynthesisStarted()
                performWebSocketSynthesis(text, miniMaxConfig, params, listener)
                if (!isCancelled && !hasCompleted) {
                    hasCompleted = true
                    listener.onSynthesisCompleted()
                }
                logInfo("Synthesis completed successfully")
            } catch (e: Exception) {
                if (!isCancelled) {
                    logError("Synthesis error", e)
                    listener.onError(e.message ?: "合成失败")
                }
            }
        }
    }

    /**
     * 通过 WebSocket 执行完整的语音合成流程
     */
    private suspend fun performWebSocketSynthesis(
        text: String,
        config: MiniMaxTtsConfig,
        params: SynthesisParams,
        listener: TtsSynthesisListener
    ) {
        val pipeClosed = AtomicBoolean(false)
        val pipedOutputStream = PipedOutputStream()
        val pipedInputStream = withContext(Dispatchers.IO) {
            PipedInputStream(pipedOutputStream, PIPE_BUFFER_SIZE)
        }

        val decodeJob = engineScope.launch(Dispatchers.Default) {
            decodeMp3Stream(pipedInputStream, listener)
        }

        val connectionDeferred = CompletableDeferred<WebSocket>()
        val taskStartedDeferred = CompletableDeferred<Unit>()
        val taskFinishedDeferred = CompletableDeferred<Unit>()
        val errorDeferred = CompletableDeferred<String>()

        val wsListener = MiniMaxWebSocketListener(
            pipedOutputStream = pipedOutputStream,
            pipeClosed = pipeClosed,
            connectionDeferred = connectionDeferred,
            taskStartedDeferred = taskStartedDeferred,
            taskFinishedDeferred = taskFinishedDeferred,
            errorDeferred = errorDeferred,
            config = config,
            params = params
        )

        try {
            val request = Request.Builder()
                .url(WSS_URL)
                .header("Authorization", "Bearer ${config.apiKey}")
                .build()

            currentWebSocket = client.newWebSocket(request, wsListener)

            val webSocket = connectionDeferred.await()

            if (isCancelled) {
                webSocket.close(1000, "Cancelled")
                return
            }

            wsListener.sendTaskStart(webSocket)

            taskStartedDeferred.await()

            if (isCancelled) {
                webSocket.close(1000, "Cancelled")
                return
            }

            val textChunks = splitTextIntoChunks(text, MAX_TEXT_LENGTH)
            logDebug("Text split into ${textChunks.size} chunks for WebSocket streaming")

            wsListener.sendTextChunks(webSocket, textChunks)

            select {
                taskFinishedDeferred.onAwait { }
                errorDeferred.onAwait { errorMsg ->
                    logError("WebSocket task failed: $errorMsg")
                    listener.onError(errorMsg)
                }
            }
        } catch (e: Exception) {
            if (!isCancelled) {
                logError("WebSocket synthesis error", e)
                listener.onError(e.message ?: "WebSocket连接失败")
            }
        } finally {
            try {
                withContext(Dispatchers.IO) {
                    pipedOutputStream.flush()
                    pipedOutputStream.close()
                }
            } catch (_: Exception) {
            }
            pipeClosed.set(true)
            decodeJob.join()
            currentWebSocket = null
        }
    }

    /**
     * WebSocket 事件监听器
     */
    inner class MiniMaxWebSocketListener(
        private val pipedOutputStream: PipedOutputStream,
        private val pipeClosed: AtomicBoolean,
        private val connectionDeferred: CompletableDeferred<WebSocket>,
        private val taskStartedDeferred: CompletableDeferred<Unit>,
        private val taskFinishedDeferred: CompletableDeferred<Unit>,
        private val errorDeferred: CompletableDeferred<String>,
        private val config: MiniMaxTtsConfig,
        private val params: SynthesisParams
    ) : WebSocketListener() {

        private val voiceId: String by lazy {
            if (config.voiceId.isNotEmpty()) {
                extractRealVoiceName(config.voiceId) ?: config.voiceId
            } else {
                voiceIds.firstOrNull() ?: "male-qn-qingse"
            }
        }

        override fun onOpen(webSocket: WebSocket, response: Response) {
            logDebug("WebSocket connected: ${response.code}")
            connectionDeferred.complete(webSocket)
        }

        override fun onMessage(webSocket: WebSocket, text: String) {
            if (pipeClosed.get() || isCancelled) return

            try {
                val json = JSONObject(text)
                val event = json.optString("event", "")

                when (event) {
                    "connected_success" -> {
                        logDebug("Received connected_success, session_id=${json.optString("session_id")}")
                    }
                    "task_started" -> {
                        logDebug("Received task_started")
                        if (!taskStartedDeferred.isCompleted) {
                            taskStartedDeferred.complete(Unit)
                        }
                    }
                    "task_continued" -> {
                        handleTaskContinued(json)
                    }
                    "task_finished" -> {
                        logDebug("Received task_finished")
                        if (!taskFinishedDeferred.isCompleted) {
                            taskFinishedDeferred.complete(Unit)
                        }
                    }
                    "task_failed" -> {
                        val baseResp = json.optJSONObject("base_resp")
                        val statusCode = baseResp?.optInt("status_code", -1) ?: -1
                        val statusMsg = baseResp?.optString("status_msg", "") ?: ""
                        val errorMsg = parseMiniMaxError(statusCode, statusMsg)
                        logError("Received task_failed: $errorMsg")
                        if (!errorDeferred.isCompleted) {
                            errorDeferred.complete(errorMsg)
                        }
                    }
                }
            } catch (e: Exception) {
                logError("Error processing WebSocket message: $text", e)
            }
        }

        private fun handleTaskContinued(json: JSONObject) {
            val baseResp = json.optJSONObject("base_resp")
            if (baseResp != null) {
                val statusCode = baseResp.optInt("status_code", 0)
                if (statusCode != 0) {
                    val statusMsg = baseResp.optString("status_msg", "")
                    logError("task_continued error: status_code=$statusCode, status_msg=$statusMsg")
                    if (!errorDeferred.isCompleted) {
                        errorDeferred.complete(parseMiniMaxError(statusCode, statusMsg))
                    }
                    return
                }
            }

            val dataObj = json.optJSONObject("data")
            if (dataObj != null) {
                val audioHex = dataObj.optString("audio", "")
                if (audioHex.isNotBlank()) {
                    val mp3Bytes = hexToBytes(audioHex)
                    if (mp3Bytes.isNotEmpty() && !pipeClosed.get()) {
                        try {
                            pipedOutputStream.write(mp3Bytes)
                        } catch (e: Exception) {
                            logDebug("Pipe write error: ${e.message}")
                        }
                    }
                }
            }

            val isFinal = json.optBoolean("is_final", false)
            if (isFinal) {
                val extraInfo = json.optJSONObject("extra_info")
                if (extraInfo != null) {
                    logDebug(
                        "Chunk complete: audio_length=${extraInfo.optInt("audio_length")}ms, " +
                                "usage_characters=${extraInfo.optInt("usage_characters")}"
                    )
                }
            }
        }

        override fun onFailure(webSocket: WebSocket, t: Throwable, response: Response?) {
            logError("WebSocket failure", t as? Exception ?: Exception(t))

            val errorMsg = when {
                response != null -> "WebSocket连接失败: HTTP ${response.code}"
                t.message?.contains("401", true) == true -> "鉴权失败，请检查 API Key"
                else -> "WebSocket连接失败: ${t.message}"
            }

            if (!connectionDeferred.isCompleted) {
                connectionDeferred.completeExceptionally(t)
            }
            if (!errorDeferred.isCompleted) {
                errorDeferred.complete(errorMsg)
            }
            completeAllDeferred(errorMsg)
        }

        override fun onClosing(webSocket: WebSocket, code: Int, reason: String) {
            logDebug("WebSocket closing: code=$code, reason=$reason")
            completeAllDeferred("连接关闭: $reason")
        }

        override fun onClosed(webSocket: WebSocket, code: Int, reason: String) {
            logDebug("WebSocket closed: code=$code, reason=$reason")
            completeAllDeferred("连接已关闭")
        }

        private fun completeAllDeferred(errorMsg: String) {
            pipeClosed.set(true)
            try { pipedOutputStream.close() } catch (_: Exception) {}

            if (!errorDeferred.isCompleted) {
                errorDeferred.complete(errorMsg)
            }
            if (!taskStartedDeferred.isCompleted) {
                taskStartedDeferred.completeExceptionally(Exception(errorMsg))
            }
            if (!taskFinishedDeferred.isCompleted) {
                taskFinishedDeferred.completeExceptionally(Exception(errorMsg))
            }
        }

        fun sendTaskStart(webSocket: WebSocket) {
            val speed = convertSpeechRate(params.speechRate)
            val vol = convertVolume(params.volume)
            val pitch = ((params.pitch - 100f) * 12f / 100f).roundToInt().coerceIn(-12, 12)
            val emotion = resolveEmotion(params)

            val message = JSONObject().apply {
                put("event", "task_start")
                put("model", DEFAULT_MODEL)
                put("continuous_sound", true)
                put("voice_setting", JSONObject().apply {
                    put("voice_id", voiceId)
                    put("speed", speed)
                    put("vol", vol)
                    put("pitch", pitch)
                    if (emotion.isNotBlank()) {
                        put("emotion", emotion)
                    }
                })
                put("audio_setting", JSONObject().apply {
                    put("sample_rate", audioConfig.sampleRate)
                    put("bitrate", 128000)
                    put("format", "mp3")
                    put("channel", audioConfig.channelCount)
                })
            }

            logDebug("Sending task_start: voice=$voiceId, speed=$speed, vol=$vol, pitch=$pitch")
            logDebug("task_start body: ${message.toString(2)}")
            webSocket.send(message.toString())
        }

        fun sendTextChunks(webSocket: WebSocket, chunks: List<String>) {
            for ((index, chunk) in chunks.withIndex()) {
                if (isCancelled || pipeClosed.get()) break

                val message = JSONObject().apply {
                    put("event", "task_continue")
                    put("text", chunk)
                }

                logDebug("Sending task_continue ${index + 1}/${chunks.size}, length=${chunk.length}")
                webSocket.send(message.toString())
            }

            if (!isCancelled && !pipeClosed.get()) {
                val finishMessage = JSONObject().apply {
                    put("event", "task_finish")
                }
                logDebug("Sending task_finish")
                webSocket.send(finishMessage.toString())
            }
        }
    }

    private fun hexToBytes(hex: String): ByteArray {
        return try {
            val cleanHex = hex.replace("\\s".toRegex(), "")
            if (cleanHex.length % 2 != 0) {
                logWarning("Invalid hex string length: ${cleanHex.length}")
                return ByteArray(0)
            }
            val bytes = ByteArray(cleanHex.length / 2)
            for (i in bytes.indices) {
                val index = i * 2
                bytes[i] = cleanHex.substring(index, index + 2).toInt(16).toByte()
            }
            bytes
        } catch (e: Exception) {
            logError("Failed to decode hex audio data", e)
            ByteArray(0)
        }
    }

    private fun decodeMp3Stream(inputStream: PipedInputStream, listener: TtsSynthesisListener) {
        val bitstream = Bitstream(inputStream)
        val decoder = Decoder()
        var sampleRate: Int

        try {
            while (!isCancelled) {
                val header: Header = bitstream.readFrame() ?: break

                sampleRate = header.frequency()

                val sampleBuffer = decoder.decodeFrame(header, bitstream) as SampleBuffer
                val samples = sampleBuffer.buffer
                val sampleCount = sampleBuffer.bufferLength

                if (sampleCount > 0) {
                    val pcmBytes = shortArrayToByteArray(samples, sampleCount)
                    listener.onAudioAvailable(
                        pcmBytes,
                        sampleRate,
                        AudioConfig.DEFAULT_AUDIO_FORMAT,
                        AudioConfig.DEFAULT_CHANNEL_COUNT
                    )
                }

                bitstream.closeFrame()
            }
        } catch (e: Exception) {
            logDebug("MP3 decoding finished or interrupted: ${e.message}")
        } finally {
            try {
                bitstream.close()
            } catch (_: Exception) {
            }
            try {
                inputStream.close()
            } catch (_: Exception) {
            }
        }
    }

    private fun shortArrayToByteArray(shortArray: ShortArray, length: Int): ByteArray {
        val buffer = ByteBuffer.allocate(length * 2).order(ByteOrder.LITTLE_ENDIAN)
        buffer.asShortBuffer().put(shortArray, 0, length)
        return buffer.array()
    }

    private fun resolveVoiceForLanguage(voiceId: String, language: String?): String {
        if (voiceId.isNotBlank() && voiceIds.contains(voiceId)) {
            return voiceId
        }
        return when (language?.lowercase()) {
            "zh", "zho", "chi", "cn" -> "male-qn-qingse"
            "en", "eng" -> "English_Graceful_Lady"
            else -> voiceId.ifBlank { "male-qn-qingse" }
        }
    }

    private fun resolveEmotion(params: SynthesisParams): String {
        return ""
    }

    private fun parseError(errorBody: String): String {
        return try {
            val json = JSONObject(errorBody)
            val baseResp = json.optJSONObject("base_resp")
            if (baseResp != null) {
                val statusCode = baseResp.optInt("status_code", 0)
                val statusMsg = baseResp.optString("status_msg", "")
                if (statusCode != 0) {
                    return parseMiniMaxError(statusCode, statusMsg)
                }
            }
            json.optString(
                "message",
                TtsErrorCode.getErrorMessage(TtsErrorCode.ERROR_SYNTHESIS_FAILED)
            )
        } catch (_: Exception) {
            TtsErrorCode.getErrorMessage(TtsErrorCode.ERROR_SYNTHESIS_FAILED)
        }
    }

    private fun parseMiniMaxError(statusCode: Int, statusMsg: String): String {
        return when (statusCode) {
            1000 -> "未知错误: $statusMsg"
            1001 -> "请求超时，请稍后重试"
            1002 -> "触发限流，请稍后重试"
            1004 -> "鉴权失败，请检查 API Key"
            1039 -> "触发 TPM 限流，请稍后重试"
            1042 -> "非法字符超过 10%，请检查文本内容"
            2013 -> "输入参数错误: $statusMsg"
            2201 -> "超时断开连接"
            2202 -> "非法事件: $statusMsg"
            2203 -> "空文本，已跳过"
            2204 -> "超出字符限制，已跳过"
            2205 -> "请求超限"
            else -> "语音合成失败: $statusMsg (code: $statusCode)"
        }
    }

    private fun convertSpeechRate(androidRate: Float): Float {
        return when {
            androidRate <= 50f -> 0.5f
            androidRate >= 200f -> 2.0f
            else -> androidRate / 100f
        }
    }

    private fun convertVolume(androidVolume: Float): Float {
        return androidVolume
    }

    private fun splitTextIntoChunks(text: String, maxLength: Int): List<String> {
        if (text.isEmpty()) return emptyList()
        if (text.length <= maxLength) return listOf(text)

        val chunks = mutableListOf<String>()
        var lastSplitPos = 0

        var i = 0
        while (i < text.length) {
            val remainingLength = text.length - lastSplitPos

            if (remainingLength <= maxLength) {
                chunks.add(text.substring(lastSplitPos))
                break
            }

            val isSentenceEnd = checkSentenceEnd(text, i)
            val isMidPause = checkMidPause(text, i)

            if (isSentenceEnd || isMidPause) {
                val chunkLength = i - lastSplitPos + 1
                if (chunkLength <= maxLength) {
                    chunks.add(text.substring(lastSplitPos, i + 1))
                    lastSplitPos = i + 1
                    i++
                    continue
                }
            }

            val splitPos = findBestSplitPos(text, lastSplitPos, maxLength)
            if (splitPos > lastSplitPos) {
                chunks.add(text.substring(lastSplitPos, splitPos))
                lastSplitPos = splitPos
            } else {
                chunks.add(text.substring(lastSplitPos, lastSplitPos + maxLength))
                lastSplitPos += maxLength
            }
            i = lastSplitPos
        }

        return chunks
    }

    private fun checkSentenceEnd(text: String, index: Int): Boolean {
        if (index < 0) return false
        val sentenceEnds = listOf("。", "！", "？", ".", "!", "?")
        for (ender in sentenceEnds) {
            if (text.regionMatches(index, ender, 0, ender.length)) {
                return true
            }
        }
        return false
    }

    private fun checkMidPause(text: String, index: Int): Boolean {
        if (index < 0) return false
        val midPauses = listOf("，", "、", ",", ";", "；", "：", ":")
        for (pause in midPauses) {
            if (text.regionMatches(index, pause, 0, pause.length)) {
                return true
            }
        }
        return false
    }

    private fun findBestSplitPos(text: String, startPos: Int, maxLength: Int): Int {
        val searchEnd = minOf(startPos + maxLength, text.length)

        for (i in searchEnd - 1 downTo startPos + 1) {
            if (checkMidPause(text, i)) {
                return i + 1
            }
        }

        for (i in searchEnd - 1 downTo startPos + 1) {
            val char = text[i]
            if (char == ' ' || char == '\n' || char == '\t') {
                return i + 1
            }
        }

        return searchEnd
    }

    override fun getSupportedLanguages(): Set<String> {
        return SUPPORTED_LANGUAGES.toSet()
    }

    override fun getDefaultLanguages(): Array<String> {
        return arrayOf(Locale.SIMPLIFIED_CHINESE.language, Locale.SIMPLIFIED_CHINESE.country, "")
    }

    override fun getSupportedVoices(): List<Voice> {
        val voices = mutableListOf<Voice>()

        for (langCode in getSupportedLanguages()) {
            for (voiceId in voiceIds) {
                voices.add(
                    Voice(
                        "$voiceId$VOICE_NAME_SEPARATOR$langCode",
                        Locale.forLanguageTag(langCode),
                        Voice.QUALITY_NORMAL,
                        Voice.LATENCY_NORMAL,
                        true,
                        emptySet()
                    )
                )
            }
        }
        return voices
    }

    override fun getDefaultVoiceId(
        lang: String?,
        country: String?,
        variant: String?,
        currentVoiceId: String?
    ): String {
        val defaultVoice = voiceIds.firstOrNull() ?: "male-qn-qingse"
        if (!currentVoiceId.isNullOrBlank()) {
            return "$currentVoiceId$VOICE_NAME_SEPARATOR$lang"
        }
        return "$defaultVoice$VOICE_NAME_SEPARATOR$lang"
    }

    override fun isVoiceIdCorrect(voiceId: String?): Boolean {
        if (voiceId == null) {
            return false
        }
        val realVoiceName = extractRealVoiceName(voiceId)
        return realVoiceName != null && voiceIds.contains(realVoiceName)
    }

    private fun extractRealVoiceName(androidVoiceName: String?): String? {
        if (androidVoiceName == null) return null
        return if (androidVoiceName.contains(VOICE_NAME_SEPARATOR)) {
            androidVoiceName.substringBefore(VOICE_NAME_SEPARATOR)
        } else {
            androidVoiceName
        }
    }

    override fun stop() {
        logInfo("Stopping synthesis")
        isCancelled = true
        currentWebSocket?.close(1000, "User cancelled")
        currentWebSocket = null
        synthesisJob?.cancel()
        synthesisJob = null
        hasCompleted = false
    }

    override fun release() {
        logInfo("Releasing engine")
        isCancelled = true
        currentWebSocket?.close(1000, "Engine released")
        currentWebSocket = null
        synthesisJob?.cancel()
        synthesisJob = null
        engineJob.cancel()
        super.release()
    }

    override fun isConfigured(config: BaseEngineConfig?): Boolean {
        val miniMaxConfig = config as? MiniMaxTtsConfig
        var result = false
        if (miniMaxConfig != null) {
            result = miniMaxConfig.apiKey.isNotBlank()
        }
        TtsLogger.d("$tag: isConfigured = $result")
        return result
    }

    override fun createDefaultConfig(): BaseEngineConfig {
        return MiniMaxTtsConfig()
    }

    override fun getConfigLabel(configKey: String, context: android.content.Context): String? {
        return when (configKey) {
            "api_key" -> context.getString(R.string.api_key_label)
            "voice_id" -> context.getString(R.string.voice_select_label)
            else -> null
        }
    }
}
