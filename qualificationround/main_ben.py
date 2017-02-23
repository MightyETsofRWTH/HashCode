import os

import sys
from typing import List, MutableSet
import progressbar
from sortedcontainers import SortedListWithKey


#FILE = 'videos_worth_spreading.in'
#FILE = 'trending_today.in'
FILE = 'kittens.in'
#FILE = 'me_at_the_zoo.in'


class Requests(object):
    def __init__(self, video_id, endpoint_id, req_count):
        self.video_id = video_id
        self.endpoint_id = endpoint_id
        self.req_count = req_count
        self.used = False


class Cache(object):
    def __init__(self, cache_size, cache_id):
        self.cache_size = cache_size
        self.space_used = 0
        self.cache_id = cache_id

        self.videos = set()  # type: MutableSet[Video]
        self.endpoint_latency = [None] * num_endpoints

    def add_video(self, video):
        self.space_used += video.video_size
        self.videos.add(video)


class CacheConnection(object):
    def __init__(self, latency, cache_id, endpoint_id):
        self.latency = latency
        self.cache_id = cache_id
        self.endpoint_id = endpoint_id


class Endpoint(object):
    def __init__(self, latency, cache_connections, endpoint_id):
        self.latency = latency
        self.cache_connections = SortedListWithKey(key=self.evaluate_cachecon, iterable=cache_connections)  # type: List[CacheConnection]
        self.endpoint_id = endpoint_id

    def evaluate_cachecon(self, con: CacheConnection):
        return self.latency - con.latency


class Video(object):
    def __init__(self, video_id, video_size):
        self.video_id = video_id
        self.video_size = video_size
        self.stored_on_caches = list()

    def __eq__(self, other):
        if other is None or not isinstance(other, Video):
            return False
        return other.video_id == self.video_id

    def __hash__(self):
        return self.video_id


if __name__ == '__main__':
    endpoints = list()  # type: List[Endpoint]
    caches = list()  # type: List[Cache]
    videos = list()  # type: List[Video]
    requests = SortedListWithKey(key=lambda val: val.req_count)  # type: SortedListWithKey[Requests]

    # Read files

    print("Start")

    with open(os.path.join('datasets', FILE), 'r') as _input:
        num_videos, num_endpoints, req_count, cache_count, cache_size = (int(str(x)) for x in
                                                                         next(_input).strip().split(' '))

        print("Reading")

        for cache_id in range(cache_count):
            caches.append(Cache(cache_size, cache_id))

        print("Generated")

        video_size_list = list(map(int, str(next(_input).strip()).split(' ')))
        min_video_size, max_video_size = min(video_size_list), max(video_size_list)
        videos = [Video(_id, size) for _id, size in enumerate(video_size_list)]

        print("Endpoints")

        endpoints_byid = dict()

        # parse endpoints and connections
        bar = progressbar.ProgressBar()
        for endpoint_id in bar(range(num_endpoints)):
            new_caches = list()
            latency, num_caches = list(map(int, str(next(_input).strip()).split(' ')))

            # parse connections
            conn_list = list()
            for j in range(num_caches):
                cache_id, cache_latency = list(map(int, str(next(_input).strip()).split(' ')))
                conn_list.append(CacheConnection(cache_latency, cache_id, endpoint_id))
                caches[cache_id].endpoint_latency[endpoint_id] = cache_latency

            ep = Endpoint(latency, conn_list, endpoint_id)
            endpoints.append(ep)
            endpoints_byid[endpoint_id] = ep

        print("Requests")

        bar = progressbar.ProgressBar()
        for i in bar(range(req_count)):
            video_id, endpoint_id, request_count = list(map(int, str(next(_input).strip()).split(' ')))
            requests.add(Requests(video_id, endpoint_id, request_count))

    # Solution
    CACHE_POSSIBLE_REQ_THRES = 50000

    bar = progressbar.ProgressBar()

    # Von Sicht der Caches aus Jeden Request der gegachet werden kann sortieren (request anzahl * latency gewinn)
    # np.sort(a, axis=-1, kind='quicksort', order=None)

    def x(v, e):
        for conid in bar(range(len(e.cache_connections))):
            cache = caches[e.cache_connections[conid].cache_id]
            if video_fits_in_cache(v, cache):
                cache.add_video(v)
                return

    def video_fits_in_cache(video, cache):
        return (cache.space_used + video.video_size) < cache.cache_size

    for rid in bar(range(len(requests))):
        v = videos[requests[rid].video_id]
        e = endpoints[requests[rid].endpoint_id]
        x(v, e)


    print("#############")

    if sys.platform.startswith('linux'):
        user = os.environ['USERNAME']
    elif sys.platform.startswith('win'):
        user = os.getlogin()
    else:
        user = "unknown"

    with open('output_{}_{}.txt'.format(user, FILE), 'w') as output_file:
        output_file.write(str(len(caches)) + '\n')
        for cache in caches:
            output_file.write("{} {}\n".format(cache.cache_id, ' '.join(map(lambda _v: str(_v.video_id), cache.videos))))
    print()
