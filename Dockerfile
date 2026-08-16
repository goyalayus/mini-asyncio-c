FROM alpine:3.22

RUN apk add --no-cache g++ musl-dev

WORKDIR /srv
COPY 00-mini-epoll-server.c 01-ready-queue.c 02-timers.c \
     03-read-future.c 04-final-async-server.c \
     05-final-async-server.cpp load-test.sh ./

RUN cc -std=c11 -Wall -Wextra -Wpedantic -Werror \
      -o mini-baseline 00-mini-epoll-server.c \
 && cc -std=c11 -Wall -Wextra -Wpedantic -Werror \
      -o mini-ready 01-ready-queue.c \
 && cc -std=c11 -Wall -Wextra -Wpedantic -Werror \
      -o mini-timers 02-timers.c \
 && cc -std=c11 -Wall -Wextra -Wpedantic -Werror \
      -o mini-read-future 03-read-future.c \
 && cc -std=c11 -Wall -Wextra -Wpedantic -Werror \
      -o mini-final 04-final-async-server.c \
 && c++ -std=c++17 -Wall -Wextra -Wpedantic -Werror \
      -o mini-final-cpp 05-final-async-server.cpp

EXPOSE 8080
CMD ["./mini-final-cpp"]
