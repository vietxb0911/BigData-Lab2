from mrjob.job import MRJob

class WordCount(MRJob):
    def mapper(self, _, line): # ứng với bước map trong mapreduce
        for word in line.split(): # tách câu thành các từ
            yield(word, 1)
    def reducer(self, word, count): # ứng với bước reduce trong mapreduce
        yield(word, sum(count))

if __name__ == '__main__':
    WordCount.run()