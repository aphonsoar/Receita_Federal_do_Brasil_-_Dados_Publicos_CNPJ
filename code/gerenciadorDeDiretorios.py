# -*- coding: utf-8 -*-
"""
Created on Tue Jul 29 08:53:44 2025

@author: lealp
"""
import os

def makedirs(path):
    '''
    cria path caso seja necessario
    '''
    if not os.path.exists(path):
        os.makedirs(path)
