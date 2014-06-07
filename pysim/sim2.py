
from pylab import *
from numpy import *
from numpy.random import randn
from random import choice, random, gauss
from collections import defaultdict



class hinge_model:
    def __init__(self, lmbda, dim):
        self.dim = dim
        self.w = zeros(dim)
        self.lmbda = lmbda

    def copy(self):
        m = hinge_model(self.lmbda, self.dim)
        m.w = self.w.copy()
        return m

    def set(self, other):
        self.w = other.w.copy()
        self.lmbda = other.lmbda
        self.dim = other.dim

    def randomize_model(self,  scale=1.0, density=1.0):
        for i in range(0, self.dim):
            self.w[i] = gaus(0, scale) if random() < density else 0.

    def gradient(self, x, y):
        prod = y * (self.w.dot(x))
        return self.lmbda * self.w - ((y * x) if prod < 1 else 0)

    def apply_gradient(self, grad):
        self.w -= eta * grad
        return self.w

    # def learn(self, x, y, t):
    #     eta = 1.0/(self.lmbda*(t+1))
    #     prod = y * (self.w.dot(x))
    #     grad = self.lmbda * w - ((y * x) if prod < 1 else 0)
    #     self.w -= eta*grad
    #     return self.w

    def predict(self, x):
        sign(self.w.dot(x))

    def point_loss(self, x, y):
        return max(0., 1. - y*(self.w.dot(x)))

    def data_loss(self, data):
        loss = 0.
        for (x, y) in data:
            loss += max(0., 1. - y*(self.w.dot(x)))
        return loss

    def model_loss(self):
        return pow(linalg.norm(self.w), 2)*lmbda/2

    def loss(self, data):
        dataLoss(self.w, data) + modelLoss(self.w, lmbda)

    def pred_accuracy(self, data):
        correct = 0
        for (x,y) in data:
            correct += 1 if y * self.w.dot(x) >= 0 else 0
        double(correct) / len(data)

    def average_models(self, models):
        self.lbmda = models[0].lmbda
        self.dim = len(models[0])
        new_w = zeros(dim)
        for m in models:
            new_w += m.w
        self.w = new_w / double(len(models))
        return self.w



def serial_SGD(model, data, iterations, initial_t=0):
    history = []
    for t in range(initial_t, initial_t+iterations):
        (x, y) = choice(data)
        grad = model.gradient(x, y)
        eta = 1.0/(model.lmbda*(t+1))
        model.apply_gradient(grad, eta)
        history.append(model.copy())
    return history

def batch_AVG_SGD(model, data_group, iterations, initial_t=0):
    # Advance all the models serially
    histories = []
    local_models = []
    for data in data_group:
        local_model = model.copy()
        history = serial_SGD(local_model, data, iterations, initial_t)
        histories.append(history)
        local_models.append(local_model)
    model.average_models(local_models)



def bsp_Hogwild(model_group, data_group, iterations, initial_t=0):
    # Advance all the models serially
    deltas = {}
    for (model, data) in zip(model_group, data_group):
        for t in range(initial_t, initial_t+iterations):
            (x, y) = choice(data)
            grad = model.gradient(x, y)
            eta = 1.0/(model.lmbda*(t+1))
            model.apply_gradient(grad, eta)
            deltas
            (x, y) = choice(data)
        grad = model.gradient(x, y)
        eta = 1.0/(model.lmbda*(t+1))
        model.apply_gradient(grad, eta)

        serial_SGD(model, data, iterations, initial_t)
    # Compute the averaged model
    model_group[0].average_models(model_group)
    # set all the models to be the model average
    for model in model_group:
        model.set(model_group[0])


def mini_batch_GD(model, data_group, iterations, initial_t=0):
    grad_sum = None
    total = 0
    # For each group of data, compute the gradient average
    for data in data_group:
        for t in range(initial_t, initial_t + iterations):
            (x, y) = choice(data)
            if grad_sum is None:
                grad_sum = model.gradient(x, y)
            else:
                grad_sum += model.gradient(x, y)
            total += 1
    eta = 1.0/(model.lmbda*(initial_t+1))
    model.apply_gradient(grad_sum / double(total), eta)




class Datagen:
    UNIFORM = 0
    SKEWED = 1
    SPLIT = 2
    POINT_CLOUD = 3


def gen_synth_data(num_samples, dim, nprocs, proc, params):
    noise = params['noise']
    if params['type'] == Datagen.POINT_CLOUD:
        pluscloud = array(params['offset1'] * ones(dim))
        negcloud = array(params['offset2'] * ones(dim))

        cloud_var = params['cloud_var']

        cloud_center = pluscloud if (proc_no % 2) == 0 else negcloud
        data = []
        for i in range(0, num_samples):
            # Generate X
            x = cloud_center + cloud_var * randn(dim)
            # Add bias term if necessary
            if 'bias' in params:
                x = append(x, 1.)
            # Generate
            y = 1 if isplus else -1
            if random() < noise:
                y = -y
            data.append((x, y))
        return data
    if params['type'] == Datagen.SPLIT:
        hasBias = 'bias' in params
        if 'bias' in params:
            dim += 1
        true_w = randn(dim)
        cloud_var = params['cloud_var']
        data = []
        for i in range(0, num_samples):
            # Generate X
            x = randn(dim)
            # set the bias term
            if hasBias:
                x[-1] = 1.
            y = 1 if w_train.dot(x) > 0 else -1
            if random() < noise:
                y = -y
            data.append((x,y))
        return data




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


DATA_PARAMS = {
    'type': Datagen.POINT_CLOUD,
    'noise': .1,
    'offset1': 5.,
    'offset2': 10.,
    'cloud_var': 1,
    'bias': True
    }


LMBDA = 0.1
NPOINTS = 1000
ITERATIONS = NPOINTS*4
NPROCS = 4
DIM = 3
NOISE = 0.0
LOG_RATE = 50
BSP_RATE = 500
DELTA_RATE = 10
BATCH_K = 1


GLOBAL_DATA = []
for proc in range(0, NPROCS):
    PROC_DATA[proc] = gen_data(NPOINTS, GEN_POLICY, proc, NPROCS, noise=NOISE)
    GLOBAL_DATA += PROC_DATA[proc]


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



PROC_DATA["global"] = GLOBAL_DATA

for i in range(0, len(SIMULATION_STEPS)-1):
    stamp = SIMULATION_STEPS[i]
    step_size = SIMULATION_STEPS[i+1]-stamp

    if stamp in LOGGING_STAMPS:
        for proc in range(0, NPROCS):
            PROC_PROGRESS[proc].append(hinge_loss_with_penalty(PROC_MODELS[proc], GLOBAL_DATA, LMBDA))
            MODEL_HIST[proc].append(PROC_MODELS[proc].copy())

        global_model = average_models(PROC_MODELS.values())
        global_penalty = hinge_loss_with_penalty(global_model, GLOBAL_DATA, LMBDA)

        print stamp, global_penalty

        PROC_PROGRESS["global"].append(global_penalty)
        MODEL_HIST["global"].append(global_model.copy())
        PROC_MODELS["global"] = global_model

        if global_penalty < GLOBAL_CUTOFF_PENALTY:
            break

    if stamp in BSP_STAMPS:
        global_model = average_models(PROC_MODELS.values())
        for proc in range(0, NPROCS):
            PROC_MODELS[proc] = global_model.copy()

    if stamp in DELTA_STAMPS:
        delta = zeros(DIM)
        old_global = PREV_MODELS["global"]
        if old_global is None:
            old_global = delta.copy()

        for proc in range(0, NPROCS):
            delta += (PROC_MODELS[proc]-old_global)
            PREV_MODELS[proc] = PROC_MODELS[proc]

        new_global = old_global + delta

        for proc in range(0, NPROCS):
            PROC_MODELS[proc] = new_global.copy()

        PREV_MODELS["global"] = new_global.copy()

    for proc in range(0, NPROCS):
        p_data = PROC_DATA[proc]

        this_fit = pegasos_sgd(p_data,
                               LMBDA,
                               step_size,
                               stamp,
                               PROC_MODELS[proc],
                               BATCH_K)

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
    MODEL_HIST["serial"].append(serial_model.copy())
    print stamp, serial_loss

PROC_DATA["serial"] = GLOBAL_DATA

for proc in PROC_PROGRESS:
    print proc, PROC_MODELS[proc], accuracy(PROC_MODELS[proc], GLOBAL_DATA)
    #plot_data(PROC_DATA[proc], proc)

for proc in PROC_PROGRESS:
    if not isinstance(proc, int):
        continue
    plot([m[0] for m in MODEL_HIST[proc]], [m[1] for m in MODEL_HIST[proc]], 'o-', label=proc)

legend()
show()

for proc in PROC_PROGRESS:
    fmt = '-' if proc != "global" and proc !="serial" else "o-"
    plot(LOGGING_STAMPS, PROC_PROGRESS[proc], fmt, label=proc)

xlabel("Iteration Number")
ylabel("Loss")

print PROC_PROGRESS["global"][-1]

legend()
show()
