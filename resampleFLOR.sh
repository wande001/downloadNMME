#cat > mygrid << EOF
#gridtype = lonlat
#xsize    = 80
#ysize    = 50
#xfirst   = -27.5
#xinc     = 1
#yfirst   = 75.5
#yinc     = -1
#EOF

file='tasmax_day_GFDL-FLORB01_FLORB01-P1-ECDA-v3.1-011999_r5i1p1_19990101-19991231.nc'
echo ${file#*.}
outfile=`echo "$file" | cut -d'/' -f3`
outfile=$outfile
echo $outfile
cdo sellonlatbox,-27.5,52.5,25.5,75.5 $file temp.nc
cdo -f nc4c -z zip -copy temp.nc $outfile
echo "Done"
rm temp.nc
#rm -f $file
