const { init, compress, decompress } = require('@bokuweb/zstd-wasm');

const currentDictionary = "HWl0A6pNEHO4AeCdArQj53JlvZKN8Fcwk3JcGv3tak8";
const dictionaryPath = "/dictionary/";
const match = 'match="/*", match-dest=("document" "frame")';
let dictionaryPromise = null;
let dictionary = null;

export default {
  async fetch(request, env) {
    // make sure the dictionary is loaded
    if (await fetchDictionary(request, env)) {
      const dictionaryPathname = dictionaryPath + currentDictionary + '.dat';
      const url = new URL(request.url);
      const isDictionary = url.pathname == dictionaryPathname;
      if (isDictionary) {
        // Construct the dictionary response
        let body = dictionary;
        if (body === null) {
          url.pathname = '/' + currentDictionary + '.dat';
          let dict = await env.ASSETS.fetch(url);
          body = dict.body;
        }
        return new Response(body, {
          headers: {
            "content-type": "text/plain; charset=UTF-8",
            "cache-control": "public, max-age=604800",
            "use-as-dictionary": match
          }
        });
      } else {
        // Passthrough fetch to original URL
        const original = await fetch(request);

        const dest = request.headers.get("sec-fetch-dest");
        if (dest && dest.indexOf("document") !== -1) {
          const response = new Response(original.body, original);

          // Add the link header to trigger the dictionary fetch
          response.headers.append(
            "Link",
            '<' + dictionaryPathname + '>; rel="compression-dictionary"',
          );

          // TODO: compress the response with the dictionary if it is available

          return response;
        } else {
          return original;
        }
      }
    } else {
      return await fetch(request);
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