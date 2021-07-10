namespace netstd api
namespace kotlin com.testing.api
namespace java com.testing.api

exception unsupportedName {
    1: string error
}

struct testOneRequest {
    1: string name
}

struct testOneResponse {
    1: string answer
}

service MyTestService {
    testOneResponse testMethod(1: testOneRequest req) throws (1: unsupportedName error)
}
