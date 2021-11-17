from mrjob.job import MRJob
import re

WORD_REGEX = re.compile(r"([a-zA-Z]+[-'][a-zA-Z]+)|([a-zA-z]+)")

class WordCount(MRJob):
    def mapper(self, _, line): # ứng với bước map trong mapreduce
        for word in WORD_REGEX.findall(line): # tách câu thành các từ
            yield(word, 1)
    def reducer(self, word, count): # ứng với bước reduce trong mapreduce
        yield(word, sum(count))

if __name__ == '__main__':
    WordCount.run()