const { Vaporous, By, Aggregation, Window } = require("../../Vaporous")


const main = async () => {
    let vaporous = await new Vaporous({
        // loggers: {
        //     perf: (level, event) => console[level](event)
        // }
    })
        .append([{
            _http_req_uri: 'https://dummyjson.com/users',
            _http_req_method: 'get'
        }])
        .output()
        .load_http()
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
        .interval(async (vaporous) => {
            await vaporous
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
                .eval(event => ({
                    _http_req_uri: 'https:/dummyjson.com/auth/me',
                    _http_req_method: 'get',
                    _http_req_headers: {
                        'content-type': 'application/json',
                        'authorization': JSON.parse(event._http_res_body).accessToken
                    },
                    _http_req_body: undefined
                }))
                .load_http()
                .parallel(4, vaporous => {
                    return vaporous.load_http()
                }, { mode: 'dynamic', multiThread: true })
                .table(event => {
                    const user = JSON.parse(event._http_res_body)
                    return {
                        gender: user.gender,
                        age: user.age,
                        bloodGroup: user.bloodGroup,
                        height: user.height,
                        weight: user.weight,
                        state: user.address.state
                    }
                })
                .assert((event, i, { expect }) => {
                    expect(event.gender === "male" || event.gender === "female");

                    ['age', 'height', 'weight'].forEach(item => {
                        expect(typeof event[item] === "number" && !Number.isNaN(event[item]))
                    })

                })
                .stats(new Aggregation('height', 'avg', 'avgHeight'), new By('gender'))

                .checkpoint('create', 'genderStats')
                .output()
                .toGraph('gender', 'avgHeight')
                .build('Average height', 'Table', {
                    columns: 2,
                    tab: "User Stats"
                })

                .checkpoint('retrieve', 'genderStats')
                .toGraph('gender', 'avgHeight')
                .build('Average height', 'Line', {
                    columns: 2,
                    tab: "User Stats"
                })

                .render()
                .begin()

            console.log('test')
        }, 5000)
        .begin()


    console.log(vaporous)
}

main()