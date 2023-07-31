from requests_html import HTMLSession
from pytube import YouTube
import os
import ray
import numpy as np

def extract_video_id(url):
    # Split the URL by "&pp" and take the first part
    video_id = url.split("&pp")[0]
    return video_id

def download(link, location='.'):
    # url input from user
    yt = YouTube(link)
    
    # extract only audio
    video = yt.streams.filter(only_audio=True).first()
    
    # check for destination to save file
    destination = location
    
    # download the file
    out_file = video.download(output_path=destination)
    
    # save the file
    base, ext = os.path.splitext(out_file)
    new_file = base + '.mp3'
    os.rename(out_file, new_file)
    # result of success
    print(yt.title + " has been successfully downloaded.")

@ray.remote
def download_search_results_helper(queries: list):
    session = HTMLSession()
    for query in queries:
        yt_links = []
        print('downloading videos for query: ', query)
        new_query = query.replace(' ', '+')
        url = f"https://www.youtube.com/results?search_query={new_query}"
        response = session.get(url)
        response.html.render(sleep=1, keep_page = True, scrolldown = 2)
        for links in response.html.find('a#video-title'):
            link = next(iter(links.absolute_links))
            yt_links.append(extract_video_id(link))

        response.close()

        for link in yt_links:
            try:
                download(link, './audio')
            except:
                print(f'Unable to download {link}')
    
    session.close()
    return None

def download_search_results(queries: list,
                            num_cores=4):
    # split up list so that each core receives ~the same number of queries to process
    queries = np.array(queries)
    split_queries = np.array_split(queries, num_cores)

    print('waiting for queries')
    results = []
    for query_sublist in split_queries:
        results.append(download_search_results_helper.remote(queries=query_sublist))

    _ = ray.get(results) # this line is necessary to prevent ray from automatically exiting the program once the jobs have been queued.
   
if __name__ == '__main__':
    ray.init()

    with open('words.txt', 'r') as f:
        words = f.read().split('\n')
        f.close()

    words = np.array(words)

    print('starting download_search_results')
    download_search_results(words,
                            num_cores=4)

    