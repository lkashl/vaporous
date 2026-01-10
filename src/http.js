const httpLib = require('http')
const httpsLib = require('https')

async function load_http() {

    for (let event of this.events) {
        const { _http_req_uri,
            _http_req_method,
            _http_req_headers,
            _http_req_body } = event


        const task = new Promise((resolve, reject) => {
            const lib = _http_req_uri.startsWith('https') ? httpsLib : httpLib
            const { hostname, port, pathname } = URL.parse(_http_req_uri)

            const req = lib.request({
                method: _http_req_method,
                hostname,
                port,
                path: pathname,
                headers: _http_req_headers
            }, res => {
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
