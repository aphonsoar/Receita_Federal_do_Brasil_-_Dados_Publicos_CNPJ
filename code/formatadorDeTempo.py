# -*- coding: utf-8 -*-
"""
Created on Wed Jul 30 21:27:15 2025

@author: lealp
"""

from datetime import timedelta, datetime
from dataclasses import dataclass, field

@dataclass(frozen=True)
class CustomTimeDelta:
    horas: int = field(init=True, hash=True, compare=True)
    minutos: int = field(init=True, hash=True, compare=True)
    segundos: int = field(init=True, hash=True, compare=True)
    
    def __str__(self):
        return f"Tempo decorrido: {self.horas} horas, {self.minutos} segundos e {self.segundos} segundos"

def formatarIntervalorTemporal(dt: timedelta) -> CustomTimeDelta:
    
    horas = 0
    minutos = 0
    segundos = dt.seconds
    
    while segundos > 60:
        segundos = segundos - 60
        minutos = minutos + 1
    
    while minutos > 60:
        minutos = minutos - 60
        horas +=1
        
    return CustomTimeDelta(horas, minutos, segundos)


if __name__ == "__main__":
    
    d1 = datetime(2025, 7, 30, 21, 0, 0)
    
    d2 = datetime(2025, 7, 30, 23, 45, 50)
    
    dt = d2 - d1
    
    dtcustom = formatarIntervalorTemporal(dt)
    
    print(dtcustom)