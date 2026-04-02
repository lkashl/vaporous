const httpLib = require('http')
const httpsLib = require('https')
const { HttpProxyAgent } = require('http-proxy-agent')
const { HttpsProxyAgent } = require('https-proxy-agent')

const request = async ({ uri, method, headers, body, options = {} }) => {
    const response = await new Promise((resolve, reject) => {
        const isHTTPs = uri.startsWith('https')
        const lib = isHTTPs ? httpsLib : httpLib
        const { hostname, port, pathname, search } = new URL(uri)
        const path = pathname + (search || '')

        const options = {
            method,
            hostname,
            port,
            path,
            headers
        }

        const proxy = isHTTPs ? (process.env.https_proxy || process.env.HTTPS_PROXY) : (process.env.http_proxy || process.env.HTTP_PROXY)

        if (proxy) options.agent = isHTTPs ? new HttpsProxyAgent(proxy) : new HttpProxyAgent(proxy)

        const req = lib.request(options, res => {
            let body = '';

            res.on('data', data => {
                body += data
            })

            res.on('end', () => {
                resolve({ body, statusCode: res.statusCode, headers: res.headers })
            })
        })

        if (body) req.write(body)
        req.end()
    })

    if (response.statusCode === 307) {
        return request({ uri: response.headers.location, method, headers, body, options })
    }

    return response;
}

async function load_http() {

    for (let event of this.events) {
        const res = await request({
            uri: event._http_req_uri,
            method: event._http_req_method,
            headers: event._http_req_headers,
            body: event._http_req_body
        })

        event._http_res_status = res.statusCode
        event._http_res_headers = res.headers
        event._http_res_body = res.body
    }

    return this
}

module.exports = {
    load_http
}
