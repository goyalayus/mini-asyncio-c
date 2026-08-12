FROM alpine:3.22

RUN apk add --no-cache gcc musl-dev

WORKDIR /srv
COPY 00-mini-epoll-server.c 01-ready-queue.c 02-timers.c load-test.sh ./

RUN cc -std=c11 -Wall -Wextra -Wpedantic -Werror \
      -o mini-baseline 00-mini-epoll-server.c \
 && cc -std=c11 -Wall -Wextra -Wpedantic -Werror \
      -o mini-ready 01-ready-queue.c \
 && cc -std=c11 -Wall -Wextra -Wpedantic -Werror \
      -o mini-timers 02-timers.c

EXPOSE 8080
CMD ["./mini-timers", "1"]
