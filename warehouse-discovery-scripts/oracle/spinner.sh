#!/bin/bash

# Function to show the spinner animation
show_spinner() {
    local -a chars=('/' '-' '\' '|')
    local i=0
    while true; do
        printf "\r${chars[$i]} "
        i=$(( (i+1) % ${#chars[@]} ))
        sleep 0.1
    done
}

# Function to stop the spinner
stop_spinner() {
    SPINNER_PID=$1
    kill $SPINNER_PID &>/dev/null
    wait $SPINNER_PID &>/dev/null
    printf "\r" # Clears the spinner line
}