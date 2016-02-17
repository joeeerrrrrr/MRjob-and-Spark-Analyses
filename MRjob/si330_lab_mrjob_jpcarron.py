__author__ = 'Joer'

from mrjob.job import MRJob
import re

class BiGramFreqCount(MRJob):

  ### input: self, ignored_key, in_value
  def mapper(self, _, line):
    yield "chars", len(line)
    yield "words", len(line.split())
    yield "lines", 1

    words = re.findall(r'\b[\w]+\b',line)
    for i in range(len(words)-1):
      word = words[i]
      nextword = words[i+1]
      bigram = word + " " + nextword
      yield (bigram, 1)

  ### input: self, in_key from mapper, in_value from mapper
  def reducer(self, key, values):
    yield key, sum(values)


if __name__ == '__main__':
  BiGramFreqCount.run()


#for x in zipped2:
#    step4.write(x[0] + "\t" + str(x[1]) + "\n")
#step4.close()
