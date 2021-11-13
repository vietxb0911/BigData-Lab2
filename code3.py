from mrjob.job import MRJob
from mrjob.step import MRStep

class MaxFrequencyWord(MRJob):

    def steps(self): # phương thức định nghĩa chương trình mapreduce này gồm 2 step như bên dưới
        return [
            MRStep(mapper=self.mapper_step1, reducer=self.reducer_step1), # step này để  tính word_count
            MRStep(mapper=self.mapper_step2, reducer=self.reducer_step2)  # step này để  tìm max_frequency_word
        ]

    def mapper_step1(self, _, line):
        for word in line.split():
            yield(word.lower(), 1)

    def reducer_step1(self, word, count):
        yield(word, sum(count))

    # Phương thức này khác với phương thức mapper ở trên là key ứng với None, còn value là cặp giá trị (word, word_count)
    # Vì nếu key là None thì tất cả các value ở bước shuffle sẽ được gom thành một list
    # và từ đó ta sẽ tìm ra max_frequency từ list đó
    def mapper_step2(self, word, word_count):
        yield(None, (word, word_count))

    # Phương thức reduce này lấy ra cặp giá trị (word, word_count) có giá trị word_count lớn nhất
    # Dùng hàm max() với key là phần tử thứ 2 trong cặp giá trị (word, word_count)
    def reducer_step2(self, _, pairs):
        yield(max(pairs, key=lambda x: x[1]))

if __name__ == "__main__":
    MaxFrequencyWord().run()