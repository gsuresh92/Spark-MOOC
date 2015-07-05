import os.path
baseDir = os.path.join('data')
inputPath = os.path.join( 'test', 'out.txt')
fileName = os.path.join(baseDir, inputPath)
inputPath2 = os.path.join('cs100', 'lab3')
STOPWORDS_PATH = 'stopwords_updated.txt'
split_regex = r'\W+'
stopfile = os.path.join(baseDir, inputPath2, STOPWORDS_PATH)
stopwords = set(sc.textFile(stopfile).collect())
print 'These are the stopwords: %s' % stopwords

def tokenize(string):
    """ An implementation of input string tokenization that excludes stopwords
    Args:
        string (str): input string
    Returns:
        list: a list of tokens without stopwords
    """
    return [i for i in re.split(split_regex,string.lower()) if (i !="" and i not in stopwords)]

def wordCount(wordListRDD):
    """Creates a pair RDD with word counts from an RDD of words.

    Args:
        wordListRDD (RDD of str): An RDD consisting of words.

    Returns:
        RDD of (str, int): An RDD consisting of (word, count) tuples.
    """
    return wordListRDD.map(lambda x:(x,1)).reduceByKey(add)
print wordCount(wordsRDD).collect()

piazzaRDD = (sc
                  .textFile(fileName, 8)
                  .flatMap(tokenize))
piazzaRDD.count()

#removing integers
piazzaWordsRDD = (piazzaRDD.
                              filter(lambda x: not x.isdigit() and len(x) > 1))
piazzaWordsRDD.count()

top200WordsAndCounts = wordCount(piazzaWordsRDD).takeOrdered(200,lambda (x,y) : -y)
print '\n'.join(map(lambda (w, c): '{0}: {1}'.format(w, c), top200WordsAndCounts))