import os

#FILE = 'me_at_the_zoo.in'
FILE = 'trending_today.in'


class Cache(object):
    def __init__(self, cache_size, cache_id):
        self.cache_size = cache_size
        self.spaceused = 0
        self.cache_id = cache_id


class CacheConnection(object):
    def __init__(self, latency, cache_id):
        self.latency = latency
        self.cache_id = cache_id


class Endpoint(object):
    def __init__(self, latency, cache_connections):
        self.latency = latency
        self.cache_connections = cache_connections

class Requests(object):
    def __init__(self, video_id, endpoint_id, req_count):
        self.video_id = video_id
        self.endpoint_id = endpoint_id
        self.req_count = req_count


if __name__ == '__main__':
    endpoints, caches, videos, requests = list(), list(), list(), list()

    # Read files

    with open(os.path.join('datasets', FILE), 'r') as _input:
        num_videos, num_endpoints, req_count, cache_count, cache_size = (int(str(x)) for x in next(_input).strip().split(' '))

        for i in range(cache_count):
            caches.append(Cache(cache_size, i))

        videos = list(map(int, str(next(_input).strip()).split(' ')))

        # parse endpoints and connections
        for i in range(num_endpoints):
            new_caches = list()
            latency, num_caches = list(map(int, str(next(_input).strip()).split(' ')))

            # parse connections
            conn_list = list()
            for j in range(num_caches):
                cache_id, cache_latency = list(map(int, str(next(_input).strip()).split(' ')))
                conn_list.append(CacheConnection(cache_latency, cache_id))

            endpoints.append(Endpoint(latency, conn_list))

        for i in range(req_count):
            video_id, endpoint_id, request_count = list(map(int, str(next(_input).strip()).split(' ')))
            requests.append(Requests(video_id, endpoint_id, request_count))

    # Solution

    print()