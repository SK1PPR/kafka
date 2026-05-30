[![progress-banner](https://backend.codecrafters.io/progress/kafka/b36350fb-6e8d-4414-9038-73cc7c89bfa0)](https://app.codecrafters.io/users/SK1PPR?r=2qF)

This is a starting point for C++ solutions to the
["Build Your Own Kafka" Challenge](https://codecrafters.io/challenges/kafka).

In this challenge, you'll build a toy Kafka clone that's capable of accepting
and responding to ApiVersions & Fetch API requests. You'll also learn about
encoding and decoding messages using the Kafka wire protocol. You'll also learn
about handling the network protocol, event loops, TCP sockets and more.

**Note**: If you're viewing this repo on GitHub, head over to
[codecrafters.io](https://codecrafters.io) to try the challenge.

# Passing the first stage

The entry point for your Kafka implementation is in `src/main.cpp`. Study and
uncomment the relevant code, and then run the command below to execute the tests
on our servers:

```sh
codecrafters submit
```

That's all!

# Stage 2 & beyond

Note: This section is for stages 2 and beyond.

1. Ensure you have `cmake` installed locally
1. Run `./your_program.sh` to run your Kafka broker, which is implemented in
   `src/main.cpp`.
1. Run `codecrafters submit` to submit your solution to CodeCrafters. Test
   output will be streamed to your terminal.
