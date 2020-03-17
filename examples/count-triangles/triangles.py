from pyspark import SparkContext
import sys

def toLU(x):
    ls = x.split(',')
    v1 = int(ls[0])
    v2 = int(ls[1])

    if v1 > v2:
        return [((v1, v2), True)]
    return []

def mymap(x):
    result = []
    ls = x.split(',')
    v1 = int(ls[0])
    v2 = int(ls[1])

    if v1 < v2:
        result.append(((v2, 'e1'), (v1, v2)))
    result.append(((v1, 'e2'), (v1, v2)))

    return result

def checkTriangles(lookup, vs):
    count = 0
    t1 = []
    joinKey = -1

    for v in vs:
        key = v[0][0]
        tn = v[0][1]
        v3 = v[1][1]
        if key != joinKey:
            t1 = []
            joinKey = key
        if tn == 'e1':
            t1.append(v[1][0])
        else:
            #t2.append(v[1][1])
            for v1 in t1:
                if v1 < v3:
                    if lookup.value.get((v3, v1), False):
                        count += 1

    return [count]

if __name__ == '__main__':
    fn = sys.argv[1] # filename of input
    p = int(sys.argv[2]) # parallelism

    sc = SparkContext(master="local[{}]".format(p),
                    appName="Triangle Count")

    text_file = sc.textFile(fn)
    lookup = sc.broadcast(text_file.flatMap(toLU).collectAsMap())
    count = text_file.flatMap(mymap) \
                    .repartitionAndSortWithinPartitions(p, lambda k: k[0]) \
                    .mapPartitions(lambda vs: checkTriangles(lookup, vs)) \
                    .reduce(lambda a,b: a + b)

    print(count)
