import os
from typing import List
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

        self.possible_requests = SortedListWithKey(key=self.evaluate_request)
        self.video_list = list()  # type: List[int]
        self.endpoint_latency = [None] * num_endpoints

    def evaluate_request(self, request: Requests):
        start_latency = endpoints[request.endpoint_id].latency
        for cache in videos[request.video_id].stored_on_caches:
            if cache.endpoint_latency[request.endpoint_id]:
                start_latency = min(start_latency, cache.endpoint_latency[request.endpoint_id])
        #return request.req_count * (start_latency - self.endpoint_latency[request.endpoint_id])
        return request.req_count/max_request_size + (start_latency - self.endpoint_latency[request.endpoint_id])/endpoints[request.endpoint_id].latency


class CacheConnection(object):
    def __init__(self, latency, cache_id, endpoint_id):
        self.latency = latency
        self.cache_id = cache_id
        self.endpoint_id = endpoint_id


class Endpoint(object):
    def __init__(self, latency, cache_connections, endpoint_id):
        self.latency = latency
        self.cache_connections = cache_connections  # type: List[CacheConnection]
        self.endpoint_id = endpoint_id


class Video(object):
    def __init__(self, video_id, video_size):
        self.video_id = video_id
        self.video_size = video_size
        self.stored_on_caches = list()


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

            endpoints.append(Endpoint(latency, conn_list, endpoint_id))

        print("Requests")

        max_request_size = 0
        bar = progressbar.ProgressBar()
        for i in bar(range(req_count)):
            video_id, endpoint_id, request_count = list(map(int, str(next(_input).strip()).split(' ')))
            requests.add(Requests(video_id, endpoint_id, request_count))
            max_request_size = max(max_request_size, request_count)

    # Solution
    CACHE_POSSIBLE_REQ_THRES = 50000

    bar = progressbar.ProgressBar()

    for request in bar(requests):
        for connection in endpoints[request.endpoint_id].cache_connections:
            if len(caches[connection.cache_id].possible_requests) < CACHE_POSSIBLE_REQ_THRES:
                caches[connection.cache_id].possible_requests.add(request)

    # Von Sicht der Caches aus Jeden Request der gegachet werden kann sortieren (request anzahl * latency gewinn)
    # np.sort(a, axis=-1, kind='quicksort', order=None)

    all_caches_full = False

    while not all_caches_full:
        #print(".")
        # Danach auf den Caches den besten 'gewinn' wählen, und gegebenfalls den Request von den anderen Caches Löschen.
        # "für alle den ersten besten" -> request aus den anderen caches austragen

        all_caches_full = True
        bar = progressbar.ProgressBar()
        best_possible_cache = None
        best_possible_request = None
        best_evaluate = 0
        for cache in caches:
            #print(len(cache.possible_requests))
            if len(cache.possible_requests) < 1:
                continue

            try:
                possible_request = cache.possible_requests.pop(-1)
                while possible_request.used or possible_request.video_id in cache.video_list:
                    possible_request = cache.possible_requests.pop(-1)

                cache.possible_requests.add(possible_request)
                new_possible_can = cache.possible_requests.pop(-1)
                while new_possible_can.used or new_possible_can.video_id in cache.video_list:
                    new_possible_can = cache.possible_requests.pop(-1)
                while possible_request != new_possible_can:
                    possible_request = new_possible_can
                    cache.possible_requests.add(possible_request)
                    new_possible_can = cache.possible_requests.pop(-1)
                    while new_possible_can.used or new_possible_can.video_id in cache.video_list:
                        new_possible_can = cache.possible_requests.pop(-1)
            except IndexError:
                continue
            #
            # video_id = possible_request.video_id
            # if videos[video_id].video_size + cache.space_used > cache_size:
            #     continue
            #
            # cache.video_list.append(video_id)
            # videos[video_id].stored_on_caches.append(cache)
            # cache.space_used += videos[video_id].video_size
            # possible_request.used = True
            # all_caches_full = False

            video_id = possible_request.video_id
            if videos[video_id].video_size + cache.space_used > cache_size:
                continue

            all_caches_full = False
            cache.possible_requests.add(possible_request)

            evaluate = cache.evaluate_request(possible_request)
            if not best_possible_request:
                best_possible_request = possible_request
                best_evaluate = evaluate
                best_possible_cache = cache
            elif best_evaluate < evaluate:
                best_possible_request = possible_request
                best_evaluate = evaluate
                best_possible_cache = cache

        if best_possible_request:
            best_possible_cache.possible_requests.remove(best_possible_request)
            video_id = best_possible_request.video_id

            best_possible_cache.video_list.append(video_id)
            videos[video_id].stored_on_caches.append(best_possible_cache)
            best_possible_cache.space_used += videos[video_id].video_size
            best_possible_request.used = True
            all_caches_full = False


    print("#############")

    print("Done")

    print("#############")

    with open('output_{}_{}.txt'.format(os.getlogin(), FILE), 'w') as output_file:
        output_file.write(str(len(caches)) + '\n')
        for cache in caches:
            output_file.write("{} {}\n".format(cache.cache_id, ' '.join(map(str, cache.video_list))))
    print()
