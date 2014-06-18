from sklearn import svm
from csv import reader
import random
from numpy.linalg import norm
from numpy import average, var

def sample(X, y, num):
    Xs = []
    ys = []
    for ns in random.sample(range(0, len(X)-1), num):
        Xs.append(X[ns])
        ys.append(y[ns])

    return Xs, ys


f = open("bank-full.csv")
f.readline()
data = reader(f, delimiter=';')

X = []
y = []

ys = 0

for line in data:
    xraw = [l.__hash__() for l in line[:-1]]
    xavg = average(xraw)
    xv = [x-xavg for x in xraw]
    xvar = var(xv)
    X.append([x/xvar for x in xv])
    y.append(1 if line[-1] == "yes" else -1)
    if line[-1] == "yes":
        ys +=1 

print ys/len(X)
        
models = []
scores = []
samplescores = []

for SAMPLESIZE in [100, 200, 3000, 10000, 20000, 30000]:
    for i in range(0, 10):
        Xs, ys = sample(X, y, SAMPLESIZE)
        clf = svm.LinearSVC()
        model = clf.fit(Xs, ys)
        models.append(model)
        ms = model.score(X, y)
        scores.append(ms)
        samplescores.append(model.score(Xs, ys))
    mean_model_coef = reduce(lambda x, y: x+y, [m.coef_ for m in models])/len(models)
    normvar = average([pow(norm(mean_model_coef-m.coef_, ord=2), 2) for m in models])

    print model.coef_
    
    print SAMPLESIZE, average(scores), average(samplescores), var(scores), normvar

clf = svm.LinearSVC()
model = clf.fit(X, y)
print model.score(X, y)

#print model.coef_
