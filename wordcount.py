import sys
import time
from collections import Counter


def _count_words(filename):
    w_count = {}
    with open(filename) as f:
        time0 = time.time_ns()
        text = f.read()
        words = text.split()
        del text
    w_count = Counter()
    w_count.update([w for w in words])
    # for w in words:
    #     if w in w_count.keys():
    #         w_count[w] += 1
    #     else:
    #         w_count[w] = 1
    count_time = (time.time_ns() - time0) / 1e9
    return w_count, count_time

Counter.clear

def print_sorted_words(filename, type=None):
    """Sort alphabetically first (ascending), and numeric second (descending)"""
    words, count_time = _count_words(filename)
    time0 = time.time_ns()
    if type == "alpha":
        words = sorted(words.items(), key=lambda kv: (kv[0], -kv[1]))
    elif type == "num":
        words = sorted(words.items(), key=lambda kv: -kv[1])
        # w_top = words.most_common()
    elif type == "numalpha":
        words = sorted(words.items(), key=lambda kv: (-kv[1], kv[0]))
        # w_top = words.most_common()
    elif type == "top":
        words = sorted(words.items(), key=lambda kv: (-kv[1], kv[0]))
        words = words[:50]
        # w_top = words.most_common(50)
    sort_time = (time.time_ns() - time0) / 1e9
    print(words)
    print("Count time (in seconds): ", count_time)
    print("Sort time (in seconds): ", sort_time)

def main():
    if len(sys.argv) != 3:
        print('usage: ./wordcount.py {--count | --alpha | --num | --numalpha | --top50} file')
        sys.exit(1)
    
    option = sys.argv[1]
    filename = sys.argv[2]
    if option == '--count':
        print_sorted_words(filename)
    elif option == '--top50':
        print_sorted_words(filename, type="top")
    elif option == "--numsort":
        print_sorted_words(filename, type="num")
    elif option == "--numalpha":
        print_sorted_words(filename, type="numalpha")
    else:
        print('unknown option: ' + option)
        sys.exit(1)

if __name__ == '__main__':
    main()
