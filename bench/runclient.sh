#!/bin/zsh -v

set +x

pnum=$1
rnum=$2
delay=0
tnum=1

[[ $# -ne 2 && $# -ne 3 ]] && echo "USAGE: ./run.sh <pnums> <request num>" && exit 1

[[ $1 -gt 0 && $1 -le 32 && $2 -gt 0 ]] || (echo "pnums in [1,64], and request num > 0" && exit 1)

#[[ $# -eq 3 ]] && [[ $3 -le 0 ]] && (echo "delay should be greater than 0 " && exit 1)
#
#[[ $# -eq 3 ]] && delay=$3

[[ $# -eq 3 ]] && [[ $3 -le 0 ]] && (echo "threads should be greater than 0 " && exit 1)

[[ $# -eq 3 ]] && tnum=$3

echo "running ${pnum} cores, each with ${tnum} threads, send  ${rnum} requests"

#dir="$(pwd)/dsarch/specpaxos"
#bdir="${dir}/bench"
bdir="."
#echo ${bdir}

echo "running ${1} processes, each  delay = ${delay}."

for ((i = 0; i < $pnum; i++)); do
  {
    if [[ $(delay) -ne 0 ]]; then
      $("${bdir}/client" -m "vr" -t ${tnum} -c "${bdir}/config.txt" -n "${rnum}" -w 20 -d "${delay}" -i ${i} )&
      subshell=$!
    else
      echo no delay
      $("${bdir}/client" -m vr -t ${tnum} -c "${bdir}/config.txt" -n "${rnum}" -w 20 -i ${i}) &
      subshell=$!
    fi
    echo "i=" ${i}
    taskset -p --cpu-list ${i} ${subshell}
  }

done

wait
