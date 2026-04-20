import json
import sys
import os
import urllib.request as ureq
from concurrent.futures import ThreadPoolExecutor
import pdb

# ================= config =================
download = 1       # 0 if images are already downloaded
max_workers = 16    # num_theards for downloading, adjust based on your network and CPU capabilities
# =========================================


opener = ureq.build_opener()
opener.addheaders = [('User-agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')]
ureq.install_opener(opener)

with open('dataset.json', 'r') as fp:
    data = json.load(fp)

def download_single_image(k):
    """
    k: dataset key
    """
    try:
        image_url = data[k]['imageURL']
        ext = os.path.splitext(image_url)[1]
        # align with official .jpg
        if not ext:
            ext = ".jpg"
            
        output_file = os.path.join('images', f"{k}{ext}")

        if os.path.exists(output_file):
            return  
            
        # --- downloading ---
        print(f"正在下载: {output_file}") 
        ureq.urlretrieve(image_url, output_file)
        
    except Exception as e:
        print(f"[{k}] 下载失败。原因: {e}")
        if 'output_file' in locals() and os.path.exists(output_file):
            os.remove(output_file)

if download == 1:
    os.makedirs('./images', exist_ok=True)
    
    print(f"开始多线程下载，线程数: {max_workers}...")
    
    # 使用线程池执行任务
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 将所有数据 key 提交给线程池
        executor.map(download_single_image, data.keys())
    
    print("下载任务处理完毕。")

example_keys = list(data.keys())[:3]
for k in example_keys:
    ext = os.path.splitext(data[k]['imageURL'])[1]
    imageFile = 'images/%s%s' % (k, ext)
    print('************************')
    print('Image file: %s' % (imageFile))
    print('List of questions:', data[k]['questions'])
    print('List of corresponding answers:', data[k]['answers'])
    print('Split: %s' % (data[k]['split']))
    print('*************************')

genSet = set()
for k in data.keys():
    genSet.add(data[k]['genre'])

numImages = len(data.keys())
numQApairs = 0
numWordsInQuestions = 0
numWordsInAnswers = 0
numQuestionsPerImage = 0
ANS = set()
authorSet = set()
bookSet = set()

for imgId in data.keys():
    numQApairs += len(data[imgId]['questions'])
    numQuestionsPerImage += len(data[imgId]['questions'])
    authorSet.add(data[imgId]['authorName'])
    bookSet.add(data[imgId]['title'])

    for ques in data[imgId]['questions']:
        numWordsInQuestions += len(ques.split())
    for ans in data[imgId]['answers']:
        ANS.add(ans)
        numWordsInAnswers += len(str(ans).split())

print("--------------------------------")
print("Number of Images: %d" % (numImages))
print("Number of QA pairs: %d" % (numQApairs))
print("Number of unique author: %d" % (len(authorSet)))
print("Number of unique title: %d" % (len(bookSet)))
print("Number of unique answers: %d" % (len(ANS)))
print("Number of unique genre: %d" % (len(genSet)))
print("Average question length (in words): %.2f" % (float(numWordsInQuestions)/float(numQApairs)))
print("Average answer length (in words): %.2f" % (float(numWordsInAnswers)/float(numQApairs)))
print("Average number of questions per image: %.2f" % (float(numQuestionsPerImage)/float(numImages)))
print("--------------------------------")