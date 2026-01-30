const httpLib = require('http')
const httpsLib = require('https')
const { HttpProxyAgent } = require('http-proxy-agent')
const { HttpsProxyAgent } = require('https-proxy-agent')


async function load_http() {

    for (let event of this.events) {
        const { _http_req_uri,
            _http_req_method,
            _http_req_headers,
            _http_req_body } = event


        const task = new Promise((resolve, reject) => {
            const lib = _http_req_uri.startsWith('https') ? httpsLib : httpLib
            const { hostname, port, pathname, search } = URL.parse(_http_req_uri)
            const path = pathname + (search || '')

            const options = {
                method: _http_req_method,
                hostname,
                port,
                path: pathname,
                headers: _http_req_headers
            }

            const proxy = process.env.HTTP_PROXY || process.env.HTTPS_PROXY || process.env.http_proxy || process.env.https_proxy

            if (proxy) {
                const isHttps = _http_req_uri.startsWith('https')
                options.agent = isHttps ? new HttpsProxyAgent(proxy) : new HttpProxyAgent(proxy)
            }

            const req = lib.request(options, res => {
                let body = '';

                res.on('data', data => {
                    body += data
                })

                res.on('end', () => {
                    event._http_res_body = body
                    event._http_res_status = res.statusCode
                    event._http_res_headers = res.headers
                    resolve(body)
                })
            })

            if (_http_req_body) req.write(_http_req_body)
            req.end()
        })

        await task
    }

    return this;
}

module.exports = {
    load_http
}
