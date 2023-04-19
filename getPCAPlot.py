import pandas as pd
import numpy as np
from sklearn.decomposition import PCA
import matplotlib.pyplot as plt
def plot_emb(fname, titlestr = "2 Component PCA", dataannotation=True):
    nemb = pd.read_csv(fname, header=None, index_col=0)
    lbl = list(nemb.index)
    
    pca = PCA(n_components=2)
    principalComponents = pca.fit_transform(nemb)
    principalDf = pd.DataFrame(data = principalComponents
                 , columns = ['principal component 1', 'principal component 2'])
    fig = plt.figure(figsize = (16,16))
    ax = fig.add_subplot(1,1,1) 
    ax.set_xlabel('Principal Component 1', fontsize = 15)
    ax.set_ylabel('Principal Component 2', fontsize = 15)
    ax.set_title(titlestr, fontsize = 20)
    ax.scatter(principalDf['principal component 1'],principalDf['principal component 2'],s=100)
    
    if dataannotation:
        for i, txt in enumerate(lbl):
            ax.annotate(txt, (principalDf['principal component 1'][i]+.02, principalDf['principal component 2'][i]),fontsize=20)
        ax.grid()
    fig.savefig('plot.png')
    
