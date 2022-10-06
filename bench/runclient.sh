#!/bin/zsh -v

set +x

tnum=$1
rnum=$2
delay=0

[[ $# -ne 2 && $# -ne 3 ]] && echo "USAGE: ./run.sh <tnums> <request num>" && exit 1

[[ $1 -gt 0 && $1 -le 64 && $2 -gt 0 ]] || (echo "tnums in [1,64], and request num > 0" && exit 1)

[[ $# -eq 3 ]] && [[ $3 -le 0 ]] && (echo "delay should be greater than 0 " && exit 1)

[[ $# -eq 3 ]] && delay=$3

echo "running ${tnum} cores, each send with ${rnum} requests"

#dir="$(pwd)/dsarch/specpaxos"
#bdir="${dir}/bench"
bdir="."
#echo ${bdir}

echo "running ${1} threads, delay = ${delay}."

for ((i = 0; i < $tnum; i++)); do
  {
    if [[ $(delay) -ne 0 ]]; then
      $("${bdir}/client" -m vr -c "${bdir}/config.txt" -n "${rnum}" -w 10 -d "${delay}" -i ${i}) &
      subshell=$!
    else
      echo no delay
      $("${bdir}/client" -m vr -c "${bdir}/config.txt" -n "${rnum}" -w 10 -i ${i}) &
      subshell=$!
    fi
    taskset -p --cpu-list $[7-$i] ${subshell}
  }

done

wait
