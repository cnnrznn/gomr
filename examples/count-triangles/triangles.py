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

def weirdTrans(x):
    key = x[0][0]
    tn = x[0][1]
    e = x[1]
    return (key, (tn, e))

def checkTriangles(lookup, vs):
    count = 0
    t1 = []

    for v in vs:
        tn = v[0]
        v3 = v[1][1]
        if tn == 'e1':
            t1.append(v[1][0])
        else:
            for v1 in t1:
                if v1 < v3:
                    if lookup.value.get((v3, v1), False):
                        count += 1

    return count

if __name__ == '__main__':
    fn = sys.argv[1] # filename of input
    p = int(sys.argv[2]) # parallelism

    sc = SparkContext(master="local[{}]".format(p),
                    appName="Triangle Count")

    text_file = sc.textFile(fn)
    lookup = sc.broadcast(text_file.flatMap(toLU).collectAsMap())
    count = text_file.flatMap(mymap) \
                    .map(weirdTrans) \
                    .groupByKey(p) \
                    .mapValues(lambda vs: sorted(vs, key=lambda x: x[0])) \
                    .map(lambda x: checkTriangles(lookup, x[1])) \
                    .reduce(lambda a,b: a + b)
                    #.repartitionAndSortWithinPartitions(p, lambda k: k[0]) \
                    #.mapPartitions(lambda vs: checkTriangles(lookup, vs)) \
                    #.reduce(lambda a,b: a + b)

    print(count)
