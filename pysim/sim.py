
from pylab import *
from numpy import *
from random import choice, random, gauss
from collections import defaultdict

def pegasos_sgd(data, lmbda, iterations, initial_t=0, w=None, step=1):
    if w is None:
        w = array([0]*len(data[0][0]))

    for i in range(0, iterations):
        t = initial_t+step*i
        (x, y) = choice(data)
        eta = 1.0/(lmbda*pow(t+1, 1))

        prod = y * (w.dot(x))

        if prod < 1:
            w_next = w * (1-eta*lmbda) + x*eta*y
        else:
            w_next = w*(1-eta*lmbda)

        w = w_next


    return w

def accuracy(model, data):
    correct = 0.
    for (x, y) in data:
        correct += 1 if sign(y) == sign(model.dot(x)) else 0
    return correct/len(data)
    

def hinge_loss(model, data, lmbda):
    loss = 0.
    for (x, y) in data:
        loss += max(0., 1.-(y*(model.dot(x))))
        #loss += 1 if sign(y) != sign(model.dot(x)) else 0
    return loss/len(data)

def add_penalty_to_loss(loss, model, lmbda):
    return loss + pow(linalg.norm(model), 2)*lmbda/2

def hinge_loss_with_penalty(model, data, lmbda):
    return add_penalty_to_loss(hinge_loss(model, data, lmbda), model, lmbda)

def random_model(dim, scale=1.0, sparsity=1.0):
    arr = []
    for i in range(0, dim):
        if random() < sparsity:
            arr.append(gauss(0, scale))
        else:
            arr.append(0.)

    return array(arr)

def gen_data(num_samples, true_w, policy, proc_no, nprocs, noise=0.3):
    ret = []

    dim = len(true_w)

    w_train = true_w.copy()

    if policy == Datagen.POINT_CLOUD:
        offset = 1
        
        pluscloud = array([5]*(dim-1)+[offset])

        print pluscloud
        
        negcloud = array([10]*(dim-1)+[offset])

        isplus = (proc_no % 2) == 0

        for i in range(0, num_samples):
            arrarr = []
            for j in range(0, dim-1):
                arrarr.append(gauss(0, 1.))

            x = (pluscloud if isplus else negcloud) + array(arrarr+[0])

            y = 1 if isplus else -1
            if random() < noise:
                y = -y

            ret.append((x, y))
        return ret
                
    if policy == Datagen.SPLIT:
        plusone = (proc_no % 2) == 0
        
        while(len(ret) < num_samples):
            arrarr = []
            for j in range(0, dim):
                arrarr.append(gauss(0, 1.))

            x = array(arrarr)
            pluspoint = w_train.dot(x) > 0
            if (pluspoint and plusone) or (not pluspoint and not plusone):
                y = 1 if pluspoint else -1
                if random < noise():
                    y = -y
                ret.append((x, y))
            else:
                continue
        return ret
        
            
    # fudge true_w so only the [proc*width, (proc+1)*width) entries
    # are non-zero
    if policy == Datagen.SKEWED:
        skew_width = dim/nprocs
        skew_start = proc_no*skew_width
        skew_w = true_w.copy()
        for i in range(skew_start, skew_start+skew_width):
            skew_w[i] = 0
        w_train -= skew_w

    for i in range(0, num_samples):
        arrarr = []
        for j in range(0, dim):
            arrarr.append(gauss(0, 1.))

        x = array(arrarr)

        
        y = 1 if w_train.dot(x) > 0 else -1

        if random() < noise:
            y = -y

        ret.append((x, y))

    return ret

def average_models(models):
    s = array([0.]*len(models[0]))
    for model in models:
        s += model
    return s/len(models)
    
def plot_data(p_data, model = None):
    plot([p[0][0] for p in p_data if p[1] == -1],
        [p[0][1] for p in p_data if p[1] == -1], 'o', color="red")

    plot([p[0][0] for p in p_data if p[1] == 1],
             [p[0][1] for p in p_data if p[1] == 1], '+', color="blue")

    if model is not None:
        plot([m[0] for m in MODEL_HIST[model]], [m[1] for m in MODEL_HIST[model]], 's-', color="black")
        
    show()

class Algorithms:
    AVERAGING = 0
    BSP = 1
    DELTA_BATCH = 2

class Datagen:
    UNIFORM = 0
    SKEWED = 1
    SPLIT = 2
    POINT_CLOUD = 3

LMBDA = 0.1
NPOINTS = 1000
ITERATIONS = NPOINTS
NPROCS = 5
DIM = 3
NOISE = 0.05
LOG_RATE = 100
BSP_RATE = 500
DELTA_RATE = 2

GEN_POLICY = Datagen.POINT_CLOUD

# quit if we get below this penalty
GLOBAL_CUTOFF_PENALTY = 0

ALGORITHM = Algorithms.BSP

# indexed arrays of examples
PROC_DATA = {}

# indexed arrays of hinge loss plus penalty
PROC_PROGRESS = defaultdict(list)
MODEL_HIST = defaultdict(list)
PROC_MODELS = defaultdict(lambda: None)
PREV_MODELS = defaultdict(lambda: None)

true_w = random_model(DIM)

#print true_w
#data = gen_data(NPOINTS, true_w, noise=0.)

#print hinge_loss(true_w, data, LMBDA)

#raw_input()


SIMULATION_STEPS = [0]

LOGGING_STAMPS = list(arange(LOG_RATE, ITERATIONS, LOG_RATE))

SIMULATION_STEPS += LOGGING_STAMPS


BSP_STAMPS = []
if ALGORITHM == Algorithms.BSP:
    BSP_STAMPS = list(arange(BSP_RATE, ITERATIONS+1, BSP_RATE))
    SIMULATION_STEPS += BSP_STAMPS

DELTA_STAMPS = []
if ALGORITHM == Algorithms.DELTA_BATCH:
    DELTA_STAMPS = list(arange(DELTA_RATE, ITERATIONS+1, DELTA_RATE))
    SIMULATION_STEPS += DELTA_STAMPS
    
SIMULATION_STEPS = list(set(SIMULATION_STEPS))
SIMULATION_STEPS.sort()

GLOBAL_DATA = []

for proc in range(0, NPROCS):
    PROC_DATA[proc] = gen_data(NPOINTS, true_w, GEN_POLICY, proc, NPROCS, noise=NOISE)
    GLOBAL_DATA += PROC_DATA[proc]

PROC_DATA["global"] = GLOBAL_DATA

for i in range(0, len(SIMULATION_STEPS)-1):
    stamp = SIMULATION_STEPS[i]
    step_size = SIMULATION_STEPS[i+1]-stamp

    if stamp in LOGGING_STAMPS:
        for proc in range(0, NPROCS):
            PROC_PROGRESS[proc].append(hinge_loss_with_penalty(PROC_MODELS[proc], GLOBAL_DATA, LMBDA))
            MODEL_HIST[proc].append(PROC_MODELS[proc])

        global_model = average_models(PROC_MODELS.values())
        global_penalty = hinge_loss_with_penalty(global_model, GLOBAL_DATA, LMBDA)

        print stamp, global_penalty

        PROC_PROGRESS["global"].append(global_penalty)
        MODEL_HIST["global"].append(this_fit)
        PROC_MODELS["global"] = global_model

        if global_penalty < GLOBAL_CUTOFF_PENALTY:
            break
    
    if stamp in BSP_STAMPS:
        global_model = average_models(PROC_MODELS.values())
        for proc in range(0, NPROCS):
            PROC_MODELS[proc] = global_model

    if stamp in DELTA_STAMPS:
        delta = array([0]*DIM)
        old_global = PREV_MODELS["global"]
        if old_global is None:
            old_global = delta.copy()
            
        for proc in range(0, NPROCS):
            delta += (PROC_MODELS[proc]-old_global)
            PREV_MODELS[proc] = PROC_MODELS[proc]

        new_global = old_global + delta

        for proc in range(0, NPROCS):
            PROC_MODELS[proc] = new_global
            
        PREV_MODELS["global"] = new_global
     
    for proc in range(0, NPROCS):
        p_data = PROC_DATA[proc]

        this_fit = pegasos_sgd(p_data,
                               LMBDA,
                               step_size,
                               stamp,
                               PROC_MODELS[proc])

        PROC_MODELS[proc] = this_fit
        

serial_model = None
tot = 0
for stamp in LOGGING_STAMPS:
    serial_model = pegasos_sgd(GLOBAL_DATA,
                            LMBDA,
                            LOG_RATE*NPROCS,
                            stamp,
                            serial_model,
                            step=1./NPROCS)
    tot += LOG_RATE*NPROCS
    serial_loss = hinge_loss_with_penalty(serial_model, GLOBAL_DATA, LMBDA)
    PROC_PROGRESS["serial"].append(serial_loss)
    PROC_MODELS["serial"] = serial_model
    MODEL_HIST["serial"].append(serial_model)
    print stamp, serial_loss

PROC_DATA["serial"] = GLOBAL_DATA
    
for proc in PROC_PROGRESS:
    print proc, PROC_MODELS[proc], accuracy(PROC_MODELS[proc], GLOBAL_DATA)
    #plot_data(PROC_DATA[proc], proc)
    
for proc in PROC_PROGRESS:
    fmt = '-' if proc != "global" and proc !="serial" else "o-"
    plot(LOGGING_STAMPS, PROC_PROGRESS[proc], fmt, label=proc)

xlabel("Iteration Number")
ylabel("Loss")
    
print PROC_PROGRESS["global"][-1]
    
legend()
show()


# average models


        
    
