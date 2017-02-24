import os
import pathlib
import sys

import collections
import progressbar
from sortedcontainers import SortedListWithKey
from typing import List, Set

MAXIMUM_REQUEST_COUNT = 10000


class Requests:
    def __init__(self, requests_id, video_id, endpoint_id, req_count):
        self.requests_id = requests_id
        self.video_id = video_id
        self.endpoint_id = endpoint_id
        self.req_count = req_count

    def __repr__(self):
        return "Requests({}, Video {}, Endpoint {}, Count {})".format(self.requests_id,
                                                                      self.video_id,
                                                                      self.endpoint_id,
                                                                      self.req_count)

    def __hash__(self):
        return int(self.requests_id)

    def __eq__(self, other):
        if other is None or not isinstance(other, Requests):
            return False
        return other.requests_id == self.requests_id


class CacheConnection:
    def __init__(self, latency, cache_id, endpoint_id):
        self.latency = latency
        self.cache_id = cache_id
        self.endpoint_id = endpoint_id

    def __repr__(self):
        return "Connection({} <-> {}, {}ms)".format(self.cache_id, self.endpoint_id, self.latency)


class Video:
    def __init__(self, video_id, video_size):
        self.video_id = video_id
        self.video_size = video_size

    def __eq__(self, other):
        if other is None or not isinstance(other, Video):
            return False
        return other.video_id == self.video_id

    def __hash__(self):
        return self.video_id

    def __str__(self):
        return str(self.video_id)


class Endpoint:
    def __init__(self, endpoint_id, latency, cache_connections):
        self.endpoint_id = endpoint_id
        self.latency = latency
        self.best_cachecon = SortedListWithKey(key=self.cachecon_ident, iterable=cache_connections)  # type: SortedListWithKey[CacheConnection]
        self.requests = set()  # type: Set[Requests]

    def cachecon_ident(self, con: CacheConnection):
        return self.latency - con.latency

    def add_requests(self, request: Requests):
        self.requests.add(request)

    def __hash__(self):
        return int(self.endpoint_id)

    def __eq__(self, other):
        if other is None or not isinstance(other, Endpoint):
            return False
        return other.endpoint_id == self.endpoint_id

    def __repr__(self):
        return "Endpoint({}, {}ms, Cache connections {}, Requests {})".format(self.endpoint_id,
                                                                                        self.latency,
                                                                                        list(self.best_cachecon),
                                                                                        self.requests)


EndpointConnection = collections.namedtuple('EndpointConnection', ('endpoint', 'latency'))


class Cache:
    def __init__(self, cache_id, cache_size):
        self.cache_id = cache_id
        self.cache_size = cache_size
        self.endpoints = set()  # type: Set[EndpointConnection]

        self.space_used = 0
        self.videos = set()  # type: Set[Video]

    def does_video_fit(self, video: Video):
        return (self.space_used + video.video_size) < self.cache_size

    def add_video(self, video: Video):
        self.space_used += video.video_size
        self.videos.add(video)

    def add_endpoint(self, endpoint: Endpoint, latency: int):
        self.endpoints.add(EndpointConnection(endpoint=endpoint, latency=latency))

    def __repr__(self):
        return "Cache({}, Size {}, Endpoints {})".format(self.cache_id,
                                                         self.cache_size,
                                                         self.endpoints)


class VideoRequests:
    def __init__(self, video: Video):
        self.video = video
        self.request_count = 0

    def


def run(inputfile: str):
    endpoints = list()  # type: List[Endpoint]
    caches = list()  # type: List[Cache]
    videos = list()  # type: List[Video]
    requests = SortedListWithKey(
        key=lambda _r: (MAXIMUM_REQUEST_COUNT - _r.req_count))  # type: SortedListWithKey[Requests]

    # Read files

    print("Start")

    with open(os.path.join('datasets', inputfile), 'r') as _input:
        num_videos, num_endpoints, request_count, cache_count, cache_size = (int(str(x)) for x in next(_input).strip().split(' '))

        print("Reading")

        for cache_id in range(cache_count):
            caches.append(Cache(cache_id, cache_size))

        print("Generated")

        video_size_list = list(map(int, str(next(_input).strip()).split(' ')))
        videos = [Video(_id, size) for _id, size in enumerate(video_size_list)]

        print("Endpoints")

        # parse endpoints and connections
        bar = progressbar.ProgressBar()
        for endpoint_id in bar(range(num_endpoints)):
            latency, num_caches = list(map(int, str(next(_input).strip()).split(' ')))

            # parse connections
            conn_list = list()
            for j in range(num_caches):
                cache_id, cache_latency = list(map(int, str(next(_input).strip()).split(' ')))
                conn_list.append(CacheConnection(cache_latency, cache_id, endpoint_id))

            e = Endpoint(endpoint_id, latency, conn_list)
            endpoints.append(e)
            for conn in conn_list:
                caches[conn.cache_id].add_endpoint(e, conn.latency)

        print("Requests")

        bar = progressbar.ProgressBar()
        for i in bar(range(request_count)):
            video_id, endpoint_id, num_of_requests = list(map(int, str(next(_input).strip()).split(' ')))

            r = Requests(i, video_id, endpoint_id, num_of_requests)
            requests.add(r)
            endpoints[endpoint_id].add_requests(r)

    # Solution
    print("Solution")

    bar = progressbar.ProgressBar()

    def add_video_to_best_cache(_video: Video, _endpoint: Endpoint):
        for _connection in _endpoint.best_cachecon:
            _cache = caches[_connection.cache_id]
            if _video in _cache.videos:
                return
            elif _cache.does_video_fit(_video):
                _cache.add_video(_video)
                return

    for r in bar(requests):
        v = videos[r.video_id]
        e = endpoints[r.endpoint_id]
        add_video_to_best_cache(v, e)

    for c in caches:
        print(c)
        videorequestnum = [0] * len(videos)

        for ec in c.endpoints:
            print(c, ec)
            for r in ec.endpoint.requests:
                videorequestnum[r.video_id] += r.req_count

        requested = dict()
        for video_id in range(len(videos)):
            if videorequestnum[video_id] != 0:
                requested[video_id] = videorequestnum[video_id]


    print("#############")

    if sys.platform.startswith('linux'):
        user = os.environ['USERNAME']
    elif sys.platform.startswith('win'):
        user = os.getlogin()
    else:
        user = "unknown"

    # create output folder with username
    outputfile = pathlib.Path('output_' + str(user))
    outputfile.mkdir(mode=0o755, parents=True, exist_ok=True)

    # append input name
    outputfile /= pathlib.PurePath(inputfile).with_suffix('.txt')

    with open(str(outputfile), 'w') as output_file:
        output_file.write(str(len(caches)) + '\n')
        for cache in caches:
            output_file.write(
                "{} {}\n".format(cache.cache_id, ' '.join(map(lambda _v: str(_v.video_id), cache.videos))))


if __name__ == '__main__':
    import concurrent.futures

    with concurrent.futures.ProcessPoolExecutor() as executor:
        executor.map(run, ('me_at_the_zoo.in', 'videos_worth_spreading.in', 'trending_today.in', 'kittens.in'))
