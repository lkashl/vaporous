const { Vaporous, By, Aggregation, Window } = require("../../Vaporous")


const main = async () => {
    let vaporous = await new Vaporous()
        .append([{
            _http_req_uri: 'https://dummyjson.com/users',
            _http_req_method: 'get'
        }])
        .load_http()

    await vaporous
        .table(event => {
            const users = JSON.parse(event._http_res_body)
            return { users: users.users }
        })
        .mvexpand('users')
        .filter(event => event._mvExpand_users < 10)
        .table(event => ({
            username: event.users.username,
            password: event.users.password
        }))
        .eval(item => ({
            _http_req_uri: 'https://dummyjson.com/auth/login',
            _http_req_method: 'post',
            _http_req_body: JSON.stringify(item),
            _http_req_headers: {
                'content-type': 'application/json'
            },
            ...item
        }))
        .load_http()

    await vaporous
        .eval(event => ({
            _http_req_uri: 'https:/dummyjson.com/auth/me',
            _http_req_method: 'get',
            _http_req_headers: {
                'content-type': 'application/json',
                'authorization': JSON.parse(event._http_res_body).accessToken
            }
        }))
        .parallel(4)
        .load_http()


    vaporous.output()

    console.log('done')
}

main()