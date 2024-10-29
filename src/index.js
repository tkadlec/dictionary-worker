const { init, compress, decompress } = require('@bokuweb/zstd-wasm');

// File name of the current dictionary asset (TODO: see if there is a way to get this dynamically)
const currentDictionary = "HWl0A6pNEHO4AeCdArQj53JlvZKN8Fcwk3JcGv3tak8";

// Psuedo-path where the dictionaries will be served from (shouldn't collide with a real directory)
const dictionaryPath = "/dictionary/";

// Match pattern for the URLs to be compressed
const match = 'match="/*", match-dest=("document" "frame")';

// Globals for managing state while waiting for the dictionary to load
let dictionaryPromise = null;
let dictionary = null;

export default {

  /*
    Main entrypoint.
    - If a fetch comes in for /dictionary/<hash>.dat, return the in-memory dictionary with an appropriate Use-As-Dictionary header.
    - If a fetch comes in with "Sec-Fetch-Dest: document" or 'frame':
      * Pass-through the fetch to get the original response.
      * Add a <link> header to trigger the dictionary fetch.
      * If the request has an "Available-Dictionary" request header that matches the current dictionary then dictionary-compress the response.
    - Otherwise, pass the fetch through to the origin.
  */
  async fetch(request, env) {
    // make sure the dictionary is loaded
    await fetchDictionary(request, env);

    // Handle the request for the dictionary itself
    const dictionaryPathname = dictionaryPath + currentDictionary + '.dat';
    const url = new URL(request.url);
    const isDictionary = url.pathname == dictionaryPathname;
    if (isDictionary) {
      let body = dictionary;
      if (body === null) {
        url.pathname = '/' + currentDictionary + '.dat';
        let dict = await env.ASSETS.fetch(url);
        body = dict.body;
      }
      return new Response(body, {
        headers: {
          "content-type": "text/plain; charset=UTF-8",  /* Can be anything but text/plain will allow for Cloudflare to apply compression */
          "cache-control": "public, max-age=604800",
          "use-as-dictionary": match
        }
      });

    } else {
      // Pass the request through to the origin
      const original = await fetch(request);

      const dest = request.headers.get("sec-fetch-dest");
      if (dest && (dest.indexOf("document") !== -1 || dest.indexOf("frame") !== -1)) {
        // Handle document (and frame) requests which need the link header added and
        // may need to be dictionary-compressed if the client announces support.
        const response = new Response(original.body, original);
        response.headers.append("Link", '<' + dictionaryPathname + '>; rel="compression-dictionary"',);

        // TODO: compress the response with the dictionary if it is available

        return response;
      } else {
        return original;
      }
    }
  }
}

/*
  Make sure the dictionary is loaded and cached into the isolate global.
  The current implementation blocks all requests until the dictionary has been loaded.
  This can be modified to fail fast and only use dictionaries after they have loaded.
 */
async function fetchDictionary(request, env) {
  if (dictionary === null) {
    if (dictionaryPromise === null) {
      let resolve;
      dictionaryPromise = new Promise((res, rej) => {
        resolve = res;
      });
      const url = new URL(request.url);
      url.pathname = '/' + currentDictionary + '.dat';
      const response = await env.ASSETS.fetch(url);
      if (response.ok) {
        dictionary = await response.arrayBuffer();
      } else {
        dictionary = false;
      }
      resolve(true);
    } else {
      await dictionaryPromise;
    }
  }
  return dictionary !== null;
}