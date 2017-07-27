import os
import datetime
import numpy as np
import uuid
import multiprocessing as mp

con = open("resampleFLOR.sh")
orgResampleFile = con.readlines()
con.close()

def endDate(model, year, month):
	endYear = year
	endMonth = month+12
	if endMonth > 12:
		endMonth += -12
		endYear += 1
	endDay = datetime.datetime(endYear, endMonth, 1) - datetime.timedelta(days=1)
	if endDay.day == 29 and model == "NCAR":
		endDay = datetime.datetime(endYear, endMonth, 1) - datetime.timedelta(days=2)
	if endDay.day == 29 and model == "CCSM":
		endDay = datetime.datetime(endYear, endMonth, 1) - datetime.timedelta(days=2)
	if endDay.day == 29 and model == "CanCM3":
		endDay = datetime.datetime(endYear, endMonth, 1) - datetime.timedelta(days=2)
	if endDay.day == 29 and model == "CanCM4":
		endDay = datetime.datetime(endYear, endMonth, 1) - datetime.timedelta(days=2)
	return "%d%.2d%.2d" %(endDay.year, endDay.month, endDay.day)

def checkEnsCFS(year, month):
	iniMonth = month
	ens = 0
	while iniMonth == month:
	  ens += 1
	  iniMonth = int(makeEnsTimeCFS(year, month, ens)[4:6])
	return ens-1

def makeEnsTimeCFS(year, month, ens):
	tempYear = 1981
	days = (datetime.datetime(tempYear, month,1) - datetime.datetime(tempYear, 1, 2)).days
	n = ens - 1
	day = n/4*5
	startDay = datetime.datetime(tempYear, 1,1) + datetime.timedelta(days = ((days/5) + 1)*5 + day)
	hour = (n/4. - n/4)*24
	return "%d%.2d%.2d%.2d" %(year, startDay.month, startDay.day, hour)

def makeURL(model, var, year, month, ens):
  if model == "GEOS5" and var == "pr":
    fileName = "%s_day_GEOS-5_%d%.2d01_r%di1p1.nc4" %(var, year, month, ens)
    url = "http://tds.ucar.edu/thredds/fileServer/datazone/nmme/output1/NASA-GMAO/GEOS-5/%d%.2d01/day/atmos/%s/%s" %(year, month, var, fileName)
  if model == "GEOS5" and var == "tas":
    fileName = "%s_day_GEOS-5_%d%.2d01_r%di1p1.nc" %(var, year, month, ens)
    url = "http://tds.ucar.edu/thredds/fileServer/datazone/nmme/output1/NASA-GMAO/GEOS-5/%d%.2d01/day/atmos/%s/%s" %(year, month, var, fileName)
  if model == "NCAR":
    fileName = "%s_day_CESM1_%d%.2d01_r%di1p1_%d%.2d00-%s.nc4" %(var, year, month, ens, year, month, endDate(model, year, month))
    url = "http://tds.ucar.edu/thredds/fileServer/datazone/nmme/output1/NCAR/CESM1/%d%.2d01/day/atmos/%s/%s" %(year, month,var, fileName)
  if model == "FLOR":
    fileName = "%s_day_GFDL-FLORB01_FLORB01-P1-ECDA-v3.1-%.2d%d_r%di1p1_%d%.2d01-%s.nc" %(var, month, year, ens, year, month, endDate(model, year, month))
    url = "http://tds.ucar.edu/thredds/fileServer/datazone/nmme/output1/NOAA-GFDL/FLORB-01/%d%.2d01/day/atmos/v20140710/%s/%s" %(year, month,var)
  if model == "CCSM":
    fileName = "%s_day_CCSM4_%d%.2d01_r%di1p1_%d%.2d01-%s.nc" %(var, year, month, ens, year, month, endDate(model, year, month))
    url = "http://tds.ucar.edu/thredds/fileServer/datazone/nmme/output1/UM-RSMAS/CCSM4/%d%.2d01/day/atmos/%s/%s" %(year, month,var, fileName)
  if model == "CanCM3":
    fileName = "%s_day_CanCM3_%d%.2d_r%di1p1_%d%.2d01-%s.nc4" %(var, year, month, ens, year, month, endDate(model, year, month))
    url = "http://tds.ucar.edu/thredds/fileServer/datazone/nmme/output1/CCCMA/CanCM3/%d%.2d01/day/atmos/v20161020/%s/%s" %(year, month,var, fileName)
  if model == "CanCM4":
    fileName = "%s_day_CanCM4_%d%.2d_r%di1p1_%d%.2d01-%s.nc4" %(var, year, month, ens, year, month, endDate(model, year, month))
    url = "http://tds.ucar.edu/thredds/fileServer/datazone/nmme/output1/CCCMA/CanCM4/%d%.2d01/day/atmos/v20161020/%s/%s" %(year, month,var, fileName)
  if model == "CFSV2":
    fileName = "%s_6hr_CFSV2-2011_%s_r1i1p1_19820101-20111227.nc" %(var, makeEnsTimeCFS(year, month,n))
    url = "http://tds.ucar.edu/thredds/fileServer/datazone/nmme/output1/NCEP/CFSV2-2011/%d%.2d01/6hr/atmos/%s/%s" %(year, month,var, fileName)
  return fileName, url

def downloadFile(model, var, year, month, n):
  fileName, url = makeURL(modelS[model], var, year, month, n)
  command = "wget -c --user-agent=wget/$WGET_VERSION/esg/2.0.82-20160725-202922 %s %s" %(fileName, url)
  if os.path.exists(fileName) == False:
    out = os.system(command)
    return fileName
  elif os.path.getsize(fileName) > 10000000.:
    return fileName
  else:
    return 0

def remapData(fileName, resampleFile = orgResampleFile):
  resampleFile[10] = "file='%s'\n" %(fileName)
  resampleFile[11] = "tempFile='%s'\n" %(str(uuid.uuid4()) + '.nc')
  randomFileName = str(uuid.uuid4()) + '.sh'
  out = open(randomFileName, "w")
  out.writelines(resampleFile)
  out.close()
  os.system('bash %s' %(randomFileName))
  os.remove('%s' %(randomFileName))


def pushData(fileName):
  var = fileName[0:6]
  os.system('scp %s niko@fire:/data/hydrology/data/niko/seasonalData/.' %(fileName))
  #os.remove('%s' %(fileName))

def procesData(model, var, year, month, n):
  fileName = downloadFile(model, var, year, month, n)
  if fileName != 0:
    remapData(fileName)
    pushData(fileName)
    return 'Complete ' + fileName
  else:
	return "Skipped %s" %(fileName) 

modelS = ["GEOS5", "NCAR", "FLOR", "CCSM", "CanCM3", "CanCM4", "CFSV2"]
varS = {}
varS["GEOS5"] = ["pr", "tas"]
varS["NCAR"] = ["pr", "tas"]
varS["FLOR"] = ["pr", "tas"]
varS["CCSM"] = ["precc", "precl","ts"]
varS["CanCM3"] = ["prlr","tas"]
varS["CanCM4"] = ["prlr", "tas"]
varS["CanCM4"] = ["prlr", "tas"]
varS["CFSV2"] = ["pr", "tas"]
ensN = [10,10, 12,10,10,10,10]

modelP = []
varP =[]
yearP =[]
monthP=[]
nP=[]

for model in range(0, len(modelS)):
  varTel = -1
  totEns = ensN[model]
  if model == 6:
    totEns = checkEnsCFS(year, month)
  for var in varS[modelS[model]]:
    varTel += 1
    for year in range(1990,2011):
      for month in range(1,totEns):
        for n in range(1,totEns+1):
          modelP.append(model)
          varP.append(var)
          yearP.append(year)
          monthP.append(month)
          nP.append(n)

nTot = len(modelP)
pool = mp.Pool(processes=10)
results = [pool.apply_async(procesData,args=(modelP[num], varP[num], yearP[num], monthP[num], nP[num])) for num in range(nTot)]

output = [p.get() for p in results]

outFile = open("filesDone.txt", "w")
for i in range(len(output)):
  outFile.writelines(output[i])

outFile.close()
