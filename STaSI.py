'''Find means and transitions in piecewise-constant time series using the STaSI algorithm
Fast Step Transition and State Identification (STaSI) for Discrete Single-Molecule Data Analysis
Bo Shuang, David Cooper, J. Nick Taylor, Lydia Kisley, Jixin Chen, Wenxiao Wang, Chun Biu Li, Tamiki Komatsuzaki, and Christy F. Landes
The Journal of Physical Chemistry Letters 2014 5 (18), 3157-3161
DOI: 10.1021/jz501435p
'''

import numpy as np
from itertools import combinations
import pandas as pd

__author__ = "Tobias Bartsch"
__email__ = "tobias-bartsch@gmx.net"


def w1(timeseries):
    '''Compute w1, the Haar wavelet transform of the lowest scale.
    w1 is just a fancy term for memory-less differences between adjacent points in the timeseries
    
    Args: 
        timeseries (np.array): time series of the data    
    '''
    return np.diff(timeseries, n=1)

def sdevFromW1(w1):
    '''Compute the standard deviation from a w1 Haar wavelet. Sdev*sqrt(2) is 68.2% from zero for the cumulative distribution of abs(w1)'''
    sortedAbsW1 = np.sort(np.abs(w1))
    if(len(sortedAbsW1)>1):
        sdev = sortedAbsW1[int(np.round(0.682*len(w1)))] / np.sqrt(2)
        return sdev
    else:
        print('w1 empty!')
        return 0


def fitSTaSIModel(data):
    '''fits the STaSI model to the data
    
    Args:
        data (np.array): the time series
    Returns:
        (fit (np.array), means (list), results (pd.DataFrame), MDLs): 
                                        fit: best fit to the data
                                        means: means of the states in the best-fitting model.
                                        results: table of identified constant segments, their beginning and end indices, their assigned states, and their assigned mean values.
                                        MDLs: np.array of mean description lengths as function of identified states. Minimizing this function is the fitting objective. Inspect this to make sure there is a well-defined minimum.
    '''
    if len(data) == 0:
        return np.nan, np.nan, np.nan, np.nan

    w1s = w1(data)
    sigma = sdevFromW1(w1s)
    if sigma == 0:
        print("we can't fit this because we can't get a decent sdev")
        return (None, None, None, None, None)
    segindices = segmentizeData(data, sigma)
    states = makeStates(data, segindices)
    means, sdevs = getMeansOfStates(data, segindices, states)
    fits = getFitFunctions(segindices, states, means)
    MDLs = MDL(data, fits, segindices, states)
    best_fit = np.argmin(MDLs)

    numstates_best = np.max(states[best_fit])
    return fits[best_fit], means[best_fit], sdevs[best_fit], _segsAndMeans(segindices, states[best_fit], means[best_fit], sdevs[best_fit]), np.asarray(MDLs)

def _segsAndMeans(segmentindices, states_one_pooling_level, means_one_pooling_level, sdevs_one_pooling_level):
    '''return a list of segments with start and end indices, their states, and their means
    
    Args:
        segmentindices (np.array): array of end-of-segment indices.
        states_one_pooling_level (list of int): assignment of each segment to a state. Must have the same length of len(segmentindices)-1. (The first value of segmentindices is 0)
                                For example, states=[1,1,1,2,2] assigns the first three segments to state 1 and the last 2 segments to state 2
                                Numbering of states starts at 1 (not at zero!).
        means_one_pooling_level (np.array of float): mean value of the data in each one of the states. (for one level of pooling only; this is a 1D array)
        sdevs_one_pooling_level (np.array of float): sdev value of the data in each one of the states. (for one level of pooling only; this is a 1D array)
    '''
    statediff = np.diff(np.asarray(states_one_pooling_level))

    starts = [0]
    stops = []
    means = []
    states = []
    sdevs = []


    for stop, state, diff in zip(segmentindices[1:-1], states_one_pooling_level[:-1], statediff):
        if diff != 0: #if diff is 0, then we want to ignore the current end index as we are remaining in the same state.
            stops.append(stop)
            starts.append(stop) #the current stop is the beginning of the next segment
            states.append(state)
            means.append(means_one_pooling_level[state-1])
            sdevs.append(sdevs_one_pooling_level[state-1])

    #now append info about the last segment
    stops.append(segmentindices[-1])
    states.append(states_one_pooling_level[-1])
    means.append(means_one_pooling_level[states[-1]-1])
    sdevs.append(sdevs_one_pooling_level[states[-1]-1])

    results = pd.DataFrame({'start': starts, 'stop': stops, 'median': means, 'sdev': sdevs, 'state': states})

    return results





def segmentizeData(data, sigma=None):
    '''detect transition points and segmentize the data until termination conditions are met.
    
    Args:
        data (np.array): the time series
        sigma: standard deviation of the data. If None we will calculate it independently for each segment from w1s (not recommended)
    Returns:
        segmentindices (np.array): array of end-of-segment indices.
    '''

    N = len(data)
    segmentindices = np.array([0, N-1]) #segments are defined as running up to (and not including) their end indices
    donesegs = np.array([False])
    done = False
    while (done == False): # are not yet done segmentizing
        segnew = [0]
        donenew = []
        for start, end, status in zip(segmentindices[0:-1], segmentindices[1:], donesegs):
            if status == False: #we need to process this segment
                if(end - start <3):
                    #this segment contains nothing. do nothing with it.
                    segnew.append(end)
                    donenew.append(True)
                    #pass
                else:
                    tpnts = _findTransitionPoint(data[start:end], sigma)
                    if tpnts is None:
                        #we are done with this segment
                        segnew.append(end)
                        donenew.append(True)
                    else:
                        #found a new transition point, split this segment
                        if(start+tpnts == end or start+tpnts == start):
                            #we are done
                            segnew.append(end)
                            donenew.append(True)
                        else:
                            segnew.append(start+tpnts)
                            segnew.append(end)
                            donenew.append(False)
                            donenew.append(False)
            else: 
                #this segment has already been processed and found not to contain any more transitions points. add it to our list
                segnew.append(end)
                donenew.append(True)
        segmentindices = segnew
        donesegs = donenew
        if all(donesegs) == True:
            done = True

    return segmentindices

def makeStates(data, segmentindices):
    '''group states into progressively fewer states (determined by the merit function of state pooling)
    
    Args:
        data (np.array): time series
        segmentindices (list of int): list of end-of-segment indices
    Returns:
        pooled_states (list of list): each sublist in the list assigns segments to a state. later entries in the list feature progressively fewer states.
    '''
    numsegs = len(segmentindices)

    states = np.arange(numsegs-1)+1 #        states (list of int): assignment of each segment to a state. Must have the same length of len(segmentindices)-1. (The first value of segmentindices is 0)
                                  #For example, states=[1,1,1,2,2] assigns the first three segments to state 1 and the last 2 segments to state 2
                                  #Numbering of states starts at 1 (not at zero!).
    pooled_states = [list(states)]
    while (max(states)>1):
        new_states = _combineTwoStates(data, segmentindices, states)
        pooled_states.append(list(new_states))
        states = new_states
    return pooled_states

def getMeansOfStates(data, segmentindices, pooled_states):
    '''compute the means of the data when pooled into different states
    Args: 
        data (np.array): time series
        segmentindices (list of int): list of end-of-segment indices
        pooled_states (list of list): each sublist in the list assigns segments to a state. later entries in the list feature progressively fewer states.

    Returns:
        (means, sdevs) (tuple of list of lists): each sublist corresponds to the means of the states for a different level of pooling
    '''
    means = []
    sdevs = []
    for i, states in enumerate(pooled_states):
        means_in_pool = []
        sdevs_in_pool = []
        statedata = _concatenateStateData(data, states, segmentindices)
        for statenum, sdata in enumerate(statedata):
            means_in_pool.append(np.median(sdata)) #NOTE: median!!!!!
            sdevs_in_pool.append(np.sqrt(np.median(sdata)+1)) #NOTE we are computing this as if it were poisson
        means.append(means_in_pool)
        sdevs.append(sdevs_in_pool)
    return means, sdevs

def getFitFunctions(segmentindices, pooled_states, means):
    '''generates plottable fit functions for each set of pooled states
    
    Args:
        segmentindices (list of int): list of end-of-segment indices
        pooled_states (list of list): each sublist in the list assigns segments to a state. later entries in the list feature progressively fewer states.
        means (list of lists): each sublist corresponds to the means of the states for a different level of pooling

    Returns:
        fit_functions (list of np.array): one fit function for each level of pooling
    '''

    fit_functions = []

    for statemeans, states in zip(means, pooled_states):
        #The last value in segmentindices is the total length of the data series
        fitfunc = np.zeros(np.max(segmentindices)+1)
        for start_index, end_index, state in zip(segmentindices[:-1], segmentindices[1:], states):
            fitfunc[start_index:end_index+1].fill(statemeans[state-1])
        fit_functions.append(fitfunc)
    
    return fit_functions


def _F(data, fit_functions, sigma):
    '''calculate the goodness of fit for every fit function in fit_functions. Uses the L1 norm.

    Args:
        data (np.array): time series
        fit_functions (list of np.array): one fit function for each level of pooling
        sigma (float): global noise
    Returns:
        Fs (list of float): one goodness of fit value F for every function in fit_functions.
    '''
    Fs =[]
    for func in fit_functions:
        Fs.append(np.sum(np.abs(data-func))/(2*sigma))
    
    return Fs

def _G(data, fit_functions, sigma, segmentindices, pooled_states):
    '''describes the complexity of the model. 
    
    Args:
        data (np.array): time series
        fit_functions (list of np.array): one fit function for each level of pooling
        sigma (float): global noise
        segmentindices (list of int): list of end-of-segment indices
        pooled_states (list of list): each sublist in the list assigns segments to a state. later entries in the list feature progressively fewer states.

    Returns:
        Gs (list of float): one complexity value per fit function
    '''
    V = np.max(data) - np.min(data) #domain size
    N = len(data) #number of data points
    N_tp = len(segmentindices)-2 #number of transition points. length of end indices minus 2 (do not count beginning and end of trace)

    Gs = []
    for fitfunc, states in zip(fit_functions, pooled_states):
        k=np.max(states) #number of states

        #get the n_i, the number of points associated with each state
        ni = np.zeros(k)
        for state, start, end in zip(states, segmentindices[0:-1], segmentindices[1:]):
            ni[state-1] = ni[state-1] + (end - start)
        
        #get the T_j, the difference of the fitting values before and after the transition position j
        T_j = []
        for trans_pos in segmentindices[1:-1]:
            t = fitfunc[trans_pos+1] - fitfunc[trans_pos-1]
            if(t > 0):
                T_j.append(t) 
        T_j = np.asarray(T_j)

        G = k/2 * np.log(1/(2*np.pi)) + k * np.log(V/sigma) + N_tp/2 * np.log(N) + 0.5 \
            * ( np.sum(np.log(ni)) + np.sum(np.log(np.power(T_j/sigma, 2))) )
        Gs.append(G)
    
    return Gs

def MDL(data, fit_functions, segmentindices, pooled_states):
    '''describes the mean description length (MDL) of the model
    
    Args:
        data (np.array): time series
        fit_functions (list of np.array): one fit function for each level of pooling
        segmentindices (list of int): list of end-of-segment indices
        pooled_states (list of list): each sublist in the list assigns segments to a state. later entries in the list feature progressively fewer states.
    
    Returns:
        MDLs (np.array of float): one MDL value per fit function
    '''
    w1s = w1(data)
    sigma = sdevFromW1(w1s)
    return np.asarray(_F(data, fit_functions, sigma)) + np.asarray(_G(data, fit_functions, sigma, segmentindices, pooled_states))

        
def _combineTwoStates(data, segmentindices, states):
    '''combine the two states with the highest merit function of their pooling. Return new state assignments.
    
    Args: 
        data (np.array): time series
        segmentindices (list of int): list of end-of-segment indices
        states (list of int): assignment of each segment to a state. Must have the same length of len(segmentindices)-1. (The first value of segmentindices is 0)
                                For example, states=[1,1,1,2,2] assigns the first three segments to state 1 and the last 2 segments to state 2
                                Numbering of states starts at 1 (not at zero!).
    '''
    numstates = np.max(states)
    
    statedata = _concatenateStateData(data, states, segmentindices)

    #get all pairs of states
    states_list = list(np.arange(numstates)+1)
    state_pairs = list(combinations(states_list,2))
    
    merits = [] #the merit function of combining a particular pair in state_pairs
    for pair in state_pairs:
        merit = _merit(statedata[pair[0]-1], statedata[pair[1]-1])
        merits.append(merit)
    
    maxmerit_index = np.argmax(np.asarray(merits))

    states = np.asarray(states)
    newstates = np.copy(states)
    newstates[np.where(states == max(state_pairs[maxmerit_index]))] = min(state_pairs[maxmerit_index])
    #we must now shift all states with number larger than max(state_pairs[maxmerit_index]) down by one
    idx = np.where(newstates > max(state_pairs[maxmerit_index]))
    newstates[idx] = newstates[idx]-1
    return newstates

def _concatenateStateData(data, states, segmentindices):
    #concatenate the segments corresponding to the same state
    numstates = np.max(states)
    statedata = [] #list of data of each state
    onestate = []
    for statenum in np.arange(numstates)+1:
        indices = np.asarray(np.where(np.asarray(states)==statenum))
        if(len(indices)==0):
            continue
        indices = np.asarray(indices).flatten() + 1 #shift the indices since the segmentindices list is longer by one (the 0th entry is '0')
        for index in indices:
            start = segmentindices[index-1]
            end = segmentindices[index]
            onestate.append(np.copy(data[start:end]))
        statedata.append(np.concatenate(onestate))
        onestate = []
    return statedata

def _merit(data_i, data_j):
    '''compute the log-likelihood merit of combining states i, j into one state. 

    Args:
        data_i: data in state i
        data_j: data in state j
    Returns:
        merit: merit function
    '''

    m_i = len(data_i) #number of points in state i
    m_j = len(data_j)
    assert(m_i > 0)
    assert(m_j > 0)
    I_i = np.mean(data_i) 
    I_j = np.mean(data_j)
    I_ij = np.mean(np.concatenate((data_i, data_j)))

    return (m_i + m_j) * I_ij**2 - (m_i * I_i**2 + m_j * I_j**2)



def _findTransitionPoint(data, sigma=None, threshold = 3.174):
    '''run t-tests checking for a transition point within the time series data

    Args:
        data (np.array): time series
        sigma: standard deviation. If None this will be calculated using w1s.
        threshold (float): threshold for t-test value. If the highest t-Test value is larger than threshold, that point is identified as a transition point.
    Returns:
        transition_index (int): index of the transition point. None if none was found.
    '''
    if sigma is None:
        w1s = w1(data)
        sigma = sdevFromW1(w1s)
    Rs = np.copy(data)
    Rs.fill(0)

    for i, _ in enumerate(data[:-1]):
        Rs[i] = _tTest(data, i, sigma)

    
    index = np.nanargmax(Rs)
    if(Rs[index] > threshold):
        return index
    else:
        return None

    

def _tTest(data, i, sigma):
    '''run one t-test to determine whether i is a transition point in data
    
    Args:
        data (np.array): time series
        i (int): index of transition point candidate
        sigma (float): global noise
    Returns:
        R (float): t-test value (transition point for R > 3.174)
    '''
    N = len(data)

    if(sigma > 0):
        R = np.abs(np.mean(data[i+1:]) - np.mean(data[:i+1])) / (sigma * np.sqrt(1/(i+1) + 1/(N-i+1)))
    else:
        R=0
    return R
