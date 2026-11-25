# Priorities
- PairDrop should be extremely simple, clean, and easy to use.
- The main user flow should never be obstructed!
- New features must be tested thoroughly before we are able to merge them.
- Stability always comes first!

# Agenda
PairDrop is a study in radical simplicity. The user interface is insanely simple. Features are chosen very carefully because complexity grows quadratically since every feature potentially interferes with each other feature. We focus very narrowly on a single use case: instant file transfer.
We are not trying to optimize for some edge-cases. We are optimizing the user flow of the average users. Don't be sad if we decline your feature request for the sake of simplicity.

If you want to learn more about simplicity you can read [Insanely Simple: The Obsession that Drives Apple's Success](https://www.amazon.com/Insanely-Simple-Ken-Segall-audiobook/dp/B007Z9686O) or [Thinking, Fast and Slow](https://www.amazon.com/Thinking-Fast-Slow-Daniel-Kahneman/dp/0374533555).

# Contributing guidelines

Run the server tests with `npm ci` and `npm test`. To run the browser tests, install Docker, Node.js 22+ and Chrome or Edge, then run these commands from the repository root.

```sh
npm ci --prefix tests/browser
npm test --prefix tests/browser
```

The visibility test checks that returning to the page reconnects its signaling socket. The transfer test builds the current checkout and restarts a disposable loopback container twice during a browser-to-browser file transfer. Set `BROWSER_CHANNEL=msedge` to use Edge or `PAIRDROP_TEST_IMAGE` to run the transfer test against an already built image.

Make sure to follow these guidelines before opening an [issue](https://github.com/schlagmichdoch/pairdrop/issues/new/choose) or a [pull request](https://github.com/schlagmichdoch/pairdrop/pulls):

- Before opening an issue of a pull request, please check if the issue or the pull request already exists.
- Pull requests for packages updates are not allowed since there is [Dependabot](https://github.com/schlagmichdoch/pairdrop/blob/master/.github/dependabot.yml) that checks them automatically.
- If you don't know how to contribute, also if you don't know JavaScript or Node.js, you can still share your awesome ideas with a new issue (feature request) and check the whole project for misspellings, too.
