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
        private const val DEFAULT_MODEL = "speech-2.8-turbo"

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

        logInfo("Starting synthesis: textLength=${text.length}, pitch=${params.pitch}, speechRate=${params.speechRate}, continuousSound=${miniMaxConfig.continuousSound}")

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
                resolveVoiceForLanguage(config.voiceId, params.language)
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

        /**
         * 处理 WebSocket 的 task_continued 事件
         *
         * 解析 JSON 中的 hex 编码 MP3 音频数据，解码后写入管道输出流
         *
         * @param json WebSocket 接收到的 JSON 消息
         */
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

        /**
         * 标记所有 Deferred 为已完成并关闭管道
         *
         * 在连接异常关闭或出错时统一处理所有异步状态
         *
         * @param errorMsg 错误消息
         */
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

        /**
         * 发送 WebSocket 任务启动消息
         *
         * 构建 task_start JSON 消息，包含音色设置、音频格式等参数
         *
         * @param webSocket 已连接的 WebSocket 实例
         */
        fun sendTaskStart(webSocket: WebSocket) {
            val speed = convertSpeechRate(params.speechRate)
            val vol = convertVolume(params.volume)
            val pitch = ((params.pitch - 100f) * 12f / 100f).roundToInt().coerceIn(-12, 12)
            val emotion = resolveEmotion(params)

            val message = JSONObject().apply {
                put("event", "task_start")
                put("model", DEFAULT_MODEL)
                put("continuous_sound", config.continuousSound)
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

            logInfo("Sending task_start: voice=$voiceId, speed=$speed, vol=$vol, pitch=$pitch, continuousSound=${config.continuousSound}")
            logInfo("task_start body: ${message.toString(2)}")
            webSocket.send(message.toString())
        }

        /**
         * 通过 WebSocket 流式发送文本片段
         *
         * 逐个发送 task_continue 消息，最后发送 task_finish 结束信号
         *
         * @param webSocket 已连接的 WebSocket 实例
         * @param chunks 分割后的文本片段列表
         */
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

    /**
     * 将十六进制字符串转换为字节数组
     *
     * 用于解码 WebSocket 返回的 hex 编码 MP3 音频数据
     *
     * @param hex 十六进制字符串
     * @return 解码后的字节数组，解码失败时返回空数组
     */
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

    /**
     * 解码 MP3 流并输出 PCM 音频数据
     *
     * 从管道输入流读取 MP3 帧，使用 JLayer 逐帧解码为 PCM，
     * 并通过 [listener] 回调输出音频数据
     *
     * @param inputStream 管道输入流，由 WebSocket 线程写入 MP3 数据
     * @param listener 音频合成监听器，接收解码后的 PCM 数据
     */
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

    /**
     * 将 ShortArray 转换为小端序字节数组
     *
     * 解码后的 PCM 样本为 short 数组，需转换为 byte 数组供音频播放器使用
     *
     * @param shortArray PCM 样本数组
     * @param length 有效样本数量
     * @return 小端序的 16-bit PCM 字节数组
     */
    private fun shortArrayToByteArray(shortArray: ShortArray, length: Int): ByteArray {
        val buffer = ByteBuffer.allocate(length * 2).order(ByteOrder.LITTLE_ENDIAN)
        buffer.asShortBuffer().put(shortArray, 0, length)
        return buffer.array()
    }

    /**
     * 根据语言解析对应的默认音色
     *
     * 当用户未指定音色时，根据目标语言选取合适的默认音色：
     * - 中文语言：使用 [male-qn-qingse]
     * - 英语语言：使用 [English_Graceful_Lady]
     * - 其他语言：回退到通用默认值
     *
     * @param voiceId 用户指定的音色 ID，为空时使用语言匹配的默认值
     * @param language 目标语言代码（zho/eng 等）
     * @return 解析后的音色 ID
     */
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

    /**
     * 解析合成参数中的情感设置
     *
     * 预留接口，当前返回空字符串表示不设置情感参数
     *
     * @param params 合成参数
     * @return 情感标识字符串，空字符串表示不设置
     */
    private fun resolveEmotion(params: SynthesisParams): String {
        return ""
    }


    /**
     * 解析 MiniMax API 错误码为中文错误消息
     *
     * 对应 MiniMax WebSocket API 的 base_resp.status_code 错误码表
     *
     * @param statusCode API 返回的状态码
     * @param statusMsg API 返回的状态消息原文
     * @return 中文错误描述消息
     */
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

    /**
     * 将 Android TTS 语速（50-200）转换为 MiniMax API 语速（0.5-2.0）
     *
     * @param androidRate Android TTS 语速值，范围 0-200
     * @return MiniMax API 语速值，范围 0.5-2.0
     */
    private fun convertSpeechRate(androidRate: Float): Float {
        return when {
            androidRate <= 50f -> 0.5f
            androidRate >= 200f -> 2.0f
            else -> androidRate / 100f
        }
    }

    /**
     * 转换音量参数
     *
     * Android TTS 音量与 MiniMax API 音量均为 0.0-1.0 范围，直接转发
     *
     * @param androidVolume Android TTS 音量值
     * @return MiniMax API 音量值
     */
    private fun convertVolume(androidVolume: Float): Float {
        return androidVolume
    }

    /**
     * 将长文本按句子边界智能分割为多个片段
     *
     * 优先在句子结尾标点（。！？.!?）处分割，
     * 其次在句中停顿标点（，、,;；：:）处分割，
     * 最后在空格或换行处分割。保证每个片段不超过 [maxLength] 字符
     *
     * @param text 待分割的原始文本
     * @param maxLength 每个片段的最大字符数
     * @return 分割后的文本片段列表
     */
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

    /**
     * 检查指定位置是否为句子结尾标点
     *
     * @param text 文本内容
     * @param index 待检查的字符位置
     * @return 是否为句子结尾标点
     */
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

    /**
     * 检查指定位置是否为句中停顿标点
     *
     * @param text 文本内容
     * @param index 待检查的字符位置
     * @return 是否为句中停顿标点
     */
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

    /**
     * 在最大长度范围内寻找最佳分割位置
     *
     * 向后搜索，优先在停顿标点处分割，其次在空白字符处分割。
     * 无法找到合适位置时返回最大长度位置
     *
     * @param text 文本内容
     * @param startPos 搜索起始位置
     * @param maxLength 最大片段长度
     * @return 最佳分割位置的索引
     */
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

    /**
     * 获取引擎支持的语言代码集合
     *
     * @return 支持的语言代码集合（zho, eng）
     */
    override fun getSupportedLanguages(): Set<String> {
        return SUPPORTED_LANGUAGES.toSet()
    }

    /**
     * 获取默认语言设置
     *
     * @return 默认使用简体中文
     */
    override fun getDefaultLanguages(): Array<String> {
        return arrayOf(Locale.SIMPLIFIED_CHINESE.language, Locale.SIMPLIFIED_CHINESE.country, "")
    }

    /**
     * 获取所有支持的声音列表
     *
     * 对每种支持的语言组合所有音色 ID，生成 Voice 对象列表
     *
     * @return 可用于 TTS 系统的 Voice 列表
     */
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

    /**
     * 获取默认声音 ID
     *
     * 当用户未指定音色时，返回内置默认音色；否则返回用户选择的音色
     *
     * @param lang 语言代码
     * @param country 国家代码
     * @param variant 变体代码
     * @param currentVoiceId 用户当前选择的声音 ID
     * @return 带语言标记的声音完整名称
     */
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

    /**
     * 验证声音 ID 是否有效
     *
     * 提取真实音色名称后检查是否在支持列表中
     *
     * @param voiceId 格式为 "voiceName::langCode" 的声音 ID
     * @return true 表示声音 ID 有效
     */
    override fun isVoiceIdCorrect(voiceId: String?): Boolean {
        if (voiceId == null) {
            return false
        }
        val realVoiceName = extractRealVoiceName(voiceId)
        return realVoiceName != null && voiceIds.contains(realVoiceName)
    }

    /**
     * 从复合声音名称中提取真实音色 ID
     *
     * 声音名称格式为 "voiceName::langCode"，此函数提取 voiceName 部分
     *
     * @param androidVoiceName 格式为 "voiceName::langCode" 的完整声音名称
     * @return 纯音色 ID，不含语言信息
     */
    private fun extractRealVoiceName(androidVoiceName: String?): String? {
        if (androidVoiceName == null) return null
        return if (androidVoiceName.contains(VOICE_NAME_SEPARATOR)) {
            androidVoiceName.substringBefore(VOICE_NAME_SEPARATOR)
        } else {
            androidVoiceName
        }
    }

    /**
     * 停止当前语音合成
     *
     * 关闭 WebSocket 连接并取消合成协程
     */
    override fun stop() {
        logInfo("Stopping synthesis")
        isCancelled = true
        currentWebSocket?.close(1000, "User cancelled")
        currentWebSocket = null
        synthesisJob?.cancel()
        synthesisJob = null
        hasCompleted = false
    }

    /**
     * 释放引擎资源
     *
     * 关闭 WebSocket 连接、取消协程并释放底层资源
     */
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

    /**
     * 检查引擎是否已完成配置
     *
     * 验证 API Key 是否已填写
     *
     * @param config 引擎配置对象
     * @return true 表示已配置
     */
    override fun isConfigured(config: BaseEngineConfig?): Boolean {
        val miniMaxConfig = config as? MiniMaxTtsConfig
        var result = false
        if (miniMaxConfig != null) {
            result = miniMaxConfig.apiKey.isNotBlank()
        }
        TtsLogger.d("$tag: isConfigured = $result")
        return result
    }

    /**
     * 创建默认引擎配置
     *
     * @return 默认的 [MiniMaxTtsConfig] 实例
     */
    override fun createDefaultConfig(): BaseEngineConfig {
        return MiniMaxTtsConfig()
    }

    /**
     * 获取配置项的中文标签
     *
     * @param configKey 配置项键名
     * @param context Android 上下文
     * @return 配置项的中文标签，不支持则返回 null
     */
    override fun getConfigLabel(configKey: String, context: android.content.Context): String? {
        return when (configKey) {
            "api_key" -> context.getString(R.string.api_key_label)
            "voice_id" -> context.getString(R.string.voice_select_label)
            "continuous_sound" -> "合成配置"
            else -> null
        }
    }
}
