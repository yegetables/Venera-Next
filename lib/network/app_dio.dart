import 'package:venera_next/foundation/global_preference_store.dart';
import 'dart:async';
import 'dart:convert';

import 'package:dio/dio.dart';
import 'package:flutter/services.dart';
import 'package:rhttp/rhttp.dart' as rhttp;
import 'package:venera_next/foundation/appdata.dart';
import 'package:venera_next/foundation/log.dart';
import 'package:venera_next/foundation/log_redaction.dart';
import 'package:venera_next/network/cache.dart';
import 'package:venera_next/network/proxy.dart';

import '../foundation/app.dart';
import 'cloudflare.dart';
import 'cookie_jar.dart';
import 'image_http3_fallback.dart';
import 'rhttp_stream_request.dart';

export 'rhttp_stream_request.dart' show RHttpCleanupFailure, RHttpCleanupError;

export 'package:dio/dio.dart';

class MyLogInterceptor extends Interceptor {
  @override
  void onError(DioException err, ErrorInterceptorHandler handler) {
    Log.error(
      "Network",
      "${err.requestOptions.method} ${err.requestOptions.path}\n$err\n${err.response?.data.toString()}",
    );
    switch (err.type) {
      case DioExceptionType.badResponse:
        var statusCode = err.response?.statusCode;
        if (statusCode != null) {
          err = err.copyWith(
            message:
                "Invalid Status Code: $statusCode. "
                "${_getStatusCodeInfo(statusCode)}",
          );
        }
      case DioExceptionType.connectionTimeout:
        err = err.copyWith(message: "Connection Timeout");
      case DioExceptionType.receiveTimeout:
        err = err.copyWith(
          message:
              "Receive Timeout: "
              "This indicates that the server is too busy to respond",
        );
      case DioExceptionType.unknown:
        if (err.error is ImageHttp3FallbackFailure) {
          err = err.copyWith(
            message: 'The image connection failed over TCP/TLS and HTTP/3.',
          );
        } else if (isImageTlsConnectionInterruption(err.error ?? err)) {
          err = err.copyWith(
            message:
                'The TLS connection was interrupted before a response. '
                'Check the server, proxy and network connection.',
          );
        } else if (err.toString().contains(
          "Connection terminated during handshake",
        )) {
          err = err.copyWith(
            message:
                "Connection terminated during handshake: "
                "Check the server, proxy and network connection.",
          );
        } else if (err.toString().contains("Connection reset by peer")) {
          err = err.copyWith(
            message:
                "Connection reset by peer: "
                "Check the server, proxy and network connection.",
          );
        }
      default:
        {}
    }
    handler.next(err);
  }

  static const errorMessages = <int, String>{
    400: "The Request is invalid.",
    401: "The Request is unauthorized.",
    403: "No permission to access the resource. Check your account or network.",
    404: "Not found.",
    429: "Too many requests. Please try again later.",
  };

  String _getStatusCodeInfo(int? statusCode) {
    if (statusCode != null && statusCode >= 500) {
      return "This is server-side error, please try again later. "
          "Do not report this issue.";
    } else {
      return errorMessages[statusCode] ?? "";
    }
  }

  @override
  void onResponse(
    Response<dynamic> response,
    ResponseInterceptorHandler handler,
  ) {
    var headers = response.headers.map.map(
      (key, value) => MapEntry(
        key.toLowerCase(),
        value.length == 1 ? value.first : value.toString(),
      ),
    );
    String content;
    if (response.data is List<int>) {
      try {
        content = utf8.decode(response.data, allowMalformed: false);
      } catch (e) {
        content = "<Bytes>\nlength:${response.data.length}";
      }
    } else {
      content = formatLogBody(response.data);
    }
    Log.addLog(
      (response.statusCode != null && response.statusCode! < 400)
          ? LogLevel.info
          : LogLevel.error,
      "Network",
      "Response ${response.realUri.toString()} ${response.statusCode}\n"
          "headers:\n${redactLogHeaders(headers)}\n$content",
    );
    handler.next(response);
  }

  @override
  void onRequest(RequestOptions options, RequestInterceptorHandler handler) {
    const String dataMask = "****** DATA_PROTECTED ******";
    final extraMask = options.extra['maskHeadersInLog'];
    final headers = redactLogHeaders(
      options.headers,
      additionallyMasked: extraMask is Iterable
          ? extraMask.whereType<String>()
          : extraMask is String
          ? extraMask.split(RegExp(r'[,\s]+'))
          : const [],
    );
    Log.info(
      "Network",
      "${options.method} ${options.uri}\n"
          "headers:\n$headers\n"
          "data:\n${options.extra["maskDataInLog"] == true ? dataMask : formatLogBody(options.data)}",
    );
    options.connectTimeout = const Duration(seconds: 15);
    options.receiveTimeout = const Duration(seconds: 15);
    options.sendTimeout = const Duration(seconds: 15);
    handler.next(options);
  }
}

class AppDio with DioMixin {
  AppDio([BaseOptions? options]) : this._(options, true);

  AppDio.forImages([BaseOptions? options]) : this._(options, true);

  AppDio._(BaseOptions? options, bool allowImageHttp3Fallback) {
    this.options = options ?? BaseOptions();
    httpClientAdapter = RHttpAdapter(
      allowImageHttp3Fallback: allowImageHttp3Fallback,
    );
    if (App.isInitialized) {
      final cache = NetworkCacheInterceptor(this);
      interceptors.add(cache.captureRequests);
      interceptors.add(
        CookieManagerSql.dynamic(() => SingleInstanceCookieJar.instance),
      );
      interceptors.add(CloudflareInterceptor());
      interceptors.add(MyLogInterceptor());
      interceptors.add(cache);
    }
  }

  static final Map<String, Future<void>> _requestTails = {};

  @override
  Future<Response<T>> request<T>(
    String path, {
    Object? data,
    Map<String, dynamic>? queryParameters,
    CancelToken? cancelToken,
    Options? options,
    ProgressCallback? onSendProgress,
    ProgressCallback? onReceiveProgress,
  }) async {
    Completer<void>? requestCompleter;
    if (options?.headers?['prevent-parallel'] == 'true') {
      final previousRequest = _requestTails[path];
      requestCompleter = Completer<void>();
      _requestTails[path] = requestCompleter.future;
      options!.headers!.remove('prevent-parallel');
      if (previousRequest != null) {
        await previousRequest;
      }
    }
    try {
      return await super.request<T>(
        path,
        data: data,
        queryParameters: queryParameters,
        cancelToken: cancelToken,
        options: options,
        onSendProgress: onSendProgress,
        onReceiveProgress: onReceiveProgress,
      );
    } finally {
      if (requestCompleter != null) {
        if (identical(_requestTails[path], requestCompleter.future)) {
          _requestTails.remove(path);
        }
        requestCompleter.complete();
      }
    }
  }
}

class RHttpAdapter implements HttpClientAdapter {
  RHttpAdapter({
    RHttpStreamCallFactory? startStreamCall,
    this.allowImageHttp3Fallback = true,
  }) : _startStreamCall = startStreamCall;

  final RHttpStreamCallFactory? _startStreamCall;
  final bool allowImageHttp3Fallback;
  final _streamRequests = <RHttpStreamRequest>{};
  final _streamPending = <Future<void>>{};
  final _streamCleanupFailures = <RHttpCleanupError>[];
  bool _streamClosed = false;

  Future<rhttp.ClientSettings> get settings async {
    var proxy = await getProxy();
    final network = GlobalPreferenceStore(appdata.settings).network;

    return rhttp.ClientSettings(
      proxySettings: proxy == null
          ? const rhttp.ProxySettings.noProxy()
          : rhttp.ProxySettings.proxy(proxy),
      redirectSettings: const rhttp.RedirectSettings.limited(5),
      timeoutSettings: const rhttp.TimeoutSettings(
        connectTimeout: Duration(seconds: 15),
        keepAliveTimeout: Duration(seconds: 60),
        keepAlivePing: Duration(seconds: 30),
      ),
      throwOnStatusCode: false,
      dnsSettings: rhttp.DnsSettings.static(
        overrides: network.effectiveDnsOverrides,
      ),
      tlsSettings: rhttp.TlsSettings(
        sni: network.sni,
        verifyCertificates: !network.ignoreBadCertificate,
      ),
    );
  }

  @override
  void close({bool force = false}) {
    _streamClosed = true;
    if (force) {
      for (final request in _streamRequests.toList()) {
        request.cancel();
      }
    }
  }

  /// Waits for actual native completion and upload/response resource release.
  /// HTTP failures and ordinary cancellation remain on the request/response;
  /// only cleanup failures are reported here, and remain failed on repeat waits.
  Future<void> waitForIdle() async {
    while (_streamPending.isNotEmpty) {
      await Future.wait(_streamPending.toList());
    }
    if (_streamCleanupFailures.isNotEmpty) {
      throw RHttpCleanupFailure(_streamCleanupFailures);
    }
  }

  @override
  Future<ResponseBody> fetch(
    RequestOptions options,
    Stream<Uint8List>? requestStream,
    Future<void>? cancelFuture,
  ) {
    if (_streamClosed) {
      return Future.error(StateError('HTTP adapter is closed'));
    }
    final cancellation = options.cancelToken?.cancelError;
    if (cancellation != null) return Future.error(cancellation);
    _setUserAgent(options);
    final request = RHttpStreamRequest(
      options: options,
      settings: Future.sync(() => settings).then(
        (settings) => settings.copyWith(
          redirectSettings: options.followRedirects
              ? rhttp.RedirectSettings.limited(options.maxRedirects)
              : const rhttp.RedirectSettings.none(),
        ),
      ),
      upload: requestStream,
      statusMessage: _getStatusMessage,
      start: _startStreamCall,
      allowImageHttp3Fallback: allowImageHttp3Fallback,
    );
    _streamRequests.add(request);
    late final Future<void> pending;
    pending = request.done
        .then<void>(
          (_) {},
          onError: (Object error, StackTrace stack) {
            if (error is RHttpCleanupFailure) {
              _streamCleanupFailures.addAll(error.failures);
            } else {
              _streamCleanupFailures.add((
                stage: 'release HTTP request',
                error: error,
                stack: stack,
              ));
            }
          },
        )
        .whenComplete(() {
          _streamRequests.remove(request);
          _streamPending.remove(pending);
        });
    _streamPending.add(pending);
    if (cancelFuture != null) {
      unawaited(cancelFuture.then((_) => request.cancel()));
    }
    return request.response;
  }

  void _setUserAgent(RequestOptions options) {
    if (options.headers['User-Agent'] == null &&
        options.headers['user-agent'] == null) {
      options.headers['User-Agent'] = "VeneraNext/v${App.version}";
    }
  }

  static String _getStatusMessage(int statusCode) {
    return switch (statusCode) {
      200 => "OK",
      201 => "Created",
      202 => "Accepted",
      204 => "No Content",
      206 => "Partial Content",
      301 => "Moved Permanently",
      302 => "Found",
      400 => "Invalid Status Code 400: The Request is invalid.",
      401 => "Invalid Status Code 401: The Request is unauthorized.",
      403 =>
        "Invalid Status Code 403: No permission to access the resource. Check your account or network.",
      404 => "Invalid Status Code 404: Not found.",
      429 =>
        "Invalid Status Code 429: Too many requests. Please try again later.",
      _ => "Invalid Status Code $statusCode",
    };
  }
}
