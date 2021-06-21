"""
downloading c4
https://commoncrawl.org/2021/05/may-2021-crawl-archive-now-available/
get wet data path first
"""
from tqdm import tqdm
from p_tqdm import p_map
import wget
import os
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
    downloaded_files = os.listdir('./')
    downloaded_c = 0
    with open(path_file, 'r') as f:
        for line in f:
            file_name = line.strip().split('/')[-1]
            if file_name in downloaded_files:
                # print(file_name)
                downloaded_c += 1
                # continue
            else:
                # print("not downloaded")
                urls.append(get_addr(line.strip()))
    # for url in tqdm(urls):
    #     wget.download(url)
    print("Downloaded files num: ", downloaded_c)
    print("To be Downloaded files num: ", len(urls))

    # p_map(wget.download, urls, num_cpus=10)


if __name__ == "__main__":
    import sys
    path_file = sys.argv[1]
    download(path_file)
