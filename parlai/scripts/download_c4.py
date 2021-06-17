"""
downloading c4
https://commoncrawl.org/2021/05/may-2021-crawl-archive-now-available/
get wet data path first
"""
from tqdm import tqdm
import wget
import multiprocessing
from multiprocessing import Pool

def get_addr(local_addr):
    """

    :param local_addr:
    :return:
    """
    head = "https://commoncrawl.s3.amazonaws.com/"
    url = head + local_addr
    return url


def download(path_file):
    """

    :param path_file:
    :return:
    """
    urls = []
    with open(path_file, 'r') as f:
        for line in f:
            urls.append(get_addr(line.strip()))
    # for url in tqdm(urls):
    #     wget.download(url)

    with Pool(10) as p:
        p.map(wget.download, urls)


if __name__ == "__main__":
    import sys
    path_file = sys.argv[1]
    download(path_file)
