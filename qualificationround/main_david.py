import os
from typing import List
import progressbar
from sortedcontainers import SortedListWithKey


#FILE = 'videos_worth_spreading.in'
#FILE = 'trending_today.in'
#FILE = 'kittens.in'
#FILE = 'me_at_the_zoo.in'

min_latency_value = 1001285472389104738290142
max_latency_value = 0
min_video_value = 1001285472389104738290142
max_video_value = 0

class CacheRequest(object):
    def __init__(self, request, cache):
        self.cache = cache
        self.request = request


class Requests(object):
    def __init__(self, video_id, endpoint_id, req_count):
        self.video_id = video_id
        self.endpoint_id = endpoint_id
        self.req_count = req_count
        self.used = False


def evaluate(cachereq: CacheRequest):
    global min_latency_value
    global max_latency_value
    global min_video_value
    global max_video_value
    request = cachereq.request
    globalcache = cachereq.cache
    start_latency = endpoints[request.endpoint_id].latency
    for cache in videos[request.video_id].stored_on_caches:
        if cache.endpoint_latency[request.endpoint_id]:
            start_latency = min(start_latency, cache.endpoint_latency[request.endpoint_id])

    latency_value = request.req_count / max_request_size * (start_latency - globalcache.endpoint_latency[request.endpoint_id]) / 1500

    video_value = videos[request.video_id].video_size / max_video_size

    factored_video_value = video_value / len(videos[request.video_id].endpoint_id_list)

    min_latency_value = min(latency_value, min_latency_value)
    max_latency_value = max(latency_value, max_latency_value)
    min_video_value = min(video_value, min_video_value)
    max_video_value = max(video_value, max_video_value)

    return latency_value * 0.9 + 0.1 * factored_video_value


class Cache(object):
    def __init__(self, cache_size, cache_id):
        self.cache_size = cache_size
        self.space_used = 0
        self.cache_id = cache_id

        self.video_list = list()  # type: List[int]
        self.endpoint_latency = [None] * num_endpoints


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
        self.endpoint_id_list = list()

def run(FILE):
    global num_videos
    global num_endpoints
    global req_count
    global cache_count
    global cache_size
    global max_request_size
    global endpoints
    global caches
    global videos
    global requests
    global min_video_size
    global max_video_size
    # Read files

    endpoints = list()  # type: List[Endpoint]
    caches = list()  # type: List[Cache]
    videos = list()  # type: List[Video]
    requests = SortedListWithKey(key=lambda val: val.req_count)  # type: SortedListWithKey[Requests]

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
            videos[video_id].endpoint_id_list.append(endpoint_id)

    # Solution
#    CACHE_POSSIBLE_REQ_THRES = 50000

    bar = progressbar.ProgressBar()

    possible_requests = SortedListWithKey(key=evaluate)

    for request in bar(requests):
        for connection in endpoints[request.endpoint_id].cache_connections:
            possible_requests.add(CacheRequest(request, caches[connection.cache_id]))

    # Von Sicht der Caches aus Jeden Request der gegachet werden kann sortieren (request anzahl * latency gewinn)
    # np.sort(a, axis=-1, kind='quicksort', order=None)

    bar = progressbar.ProgressBar()

    lower_index = 0

    count = len(possible_requests)

    for index in bar(range(count)):
        cachereq = possible_requests[count - index - 1]
        cache = cachereq.cache
        request = cachereq.request

        if request.used or request.video_id in cache.video_list:
            continue

        video_id = request.video_id
        if videos[video_id].video_size + cache.space_used > cache_size:
            continue

        del possible_requests[count - index - 1]
        possible_requests.add(cachereq)
        resorted_elements = list()
        while cachereq != possible_requests[count - index - 1]:
            resorted_elements.append(cachereq)
            cachereq = possible_requests[count - index - 1]
            del possible_requests[count - index - 1]
            possible_requests.add(cachereq)
            if cachereq in resorted_elements:
                break

        cache.video_list.append(video_id)
        videos[video_id].stored_on_caches.append(cache)
        cache.space_used += videos[video_id].video_size
        request.used = True

    print("#############")

    print("Done")

    print("#############")

    global min_latency_value
    global max_latency_value

    print(min_latency_value)
    print(max_latency_value)

    print("#############")

    global min_video_value
    global max_video_value

    print(min_video_value)
    print(max_video_value)

    print("#############")

    with open('output_{}_{}.txt'.format(os.getlogin(), FILE), 'w') as output_file:
        output_file.write(str(len(caches)) + '\n')
        for cache in caches:
            output_file.write("{} {}\n".format(cache.cache_id, ' '.join(map(str, cache.video_list))))
    print()


if __name__ == '__main__':
    #run('me_at_the_zoo.in')
    #run('videos_worth_spreading.in')
    #run('trending_today.in')
    import concurrent.futures
    with concurrent.futures.ProcessPoolExecutor() as executor:
        executor.map(run, ('me_at_the_zoo.in', 'videos_worth_spreading.in', 'trending_today.in', 'kittens.in'))
