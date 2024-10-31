import zstdlib from "../zstd-wasm-compress/bin/zstdlib.js";
import zstdwasm from "../zstd-wasm-compress/bin/zstdlib.wasm";

// File name of the current dictionary asset (TODO: see if there is a way to get this dynamically)
const currentDictionary = "HWl0A6pNEHO4AeCdArQj53JlvZKN8Fcwk3JcGv3tak8";
const currentHash = atob(currentDictionary.replaceAll('-', '+').replaceAll('_', '/'));

// Psuedo-path where the dictionaries will be served from (shouldn't collide with a real directory)
const dictionaryPath = "/dictionary/";
const dictionaryPathname = dictionaryPath + currentDictionary + '.dat';

// Match pattern for the URLs to be compressed
const match = 'match="/*", match-dest=("document" "frame")';

const dictionaryExpiration = 30 * 24 * 3600;  // 30 day expiration on the dictionary itself

// Block requests on dictionary and zstd being loaded?
const blocking = false;
const compressionLevel = 10;

// Globals for managing state while waiting for the dictionary and zstd wasm to load
let zstd = null;
let dictionaryLoaded = null;
let zstdLoaded = null;
let initialized = false;
let dictionary = null;
let dictionaryJS = null;

// Initialize wasm outside of a request context

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
  async fetch(request, env, ctx) {
    // Trigger the async dictionary load (has to be done in a request context to have access to env)
    dictionaryInit(request, env, ctx);
    zstdInit();

    // Handle the request for the dictionary itself
    const url = new URL(request.url);
    const isDictionary = url.pathname == dictionaryPathname;
    if (isDictionary) {
      return await fetchDictionary(env, url);
    } else {
      const original = await fetch(request);

      const dest = request.headers.get("sec-fetch-dest");
      if (dest && (dest.indexOf("document") !== -1 || dest.indexOf("frame") !== -1)) {
        // block on the dictionary/zstd init if necessary
        if (blocking) {
          if (zstd === null) { await zstdLoaded; }
          if (dictionary === null) { await dictionaryLoaded; }
        }

        if (supportsCompression(request) && zstd !== null && dictionary !== null) {
          return compressResponse(original, ctx);
        } else {
          const response = new Response(original.body, original);
          response.headers.append("Link", '<' + dictionaryPathname + '>; rel="compression-dictionary"',);
          return response;
        }
      } else {
        return original;
      }
    }
  }
}

/*
  Dictionary-compress the response
*/
function compressResponse(original, ctx) {

  const { readable, writable } = new TransformStream();
  ctx.waitUntil(compressStream(original.body, writable));

  // Add the appropriate headers
  const response = new Response(readable, original);
  let ver = zstd.versionNumber();
  response.headers.append("Link", '<' + dictionaryPathname + '>; rel="compression-dictionary"',);
  response.headers.set("X-Zstd-Version", ver);
  response.headers.set("Vary", 'Accept-Encoding, Available-Dictionary',);
  return response;
}

async function compressStream(readable, writable) {
  const reader = readable.getReader();
  const writer = writable.getWriter();

  // allocate a compression context and buffers before the stream starts
  let cctx = null;
  let zstdInBuff = null;
  let zstdOutBuff = null;
  try {
    cctx = zstd.createCCtx();
    if (cctx !== null) {
      const inSize = zstd.CStreamInSize();
      const outSize = 4096; // use a small 4kb output buffer to keep the compression responsive and avoid over-buffering
      console.log("Allocating zstd buffers. in: " + inSize +", out: " + outSize);
      zstdInBuff = zstd._malloc(inSize);
      zstdOutBuff = zstd._malloc(outSize);
    }
  } catch (E) {
    console.log(E);
  }

  // TODO: write dcb magic header

  // streaming compression modeled after https://github.com/facebook/zstd/blob/dev/examples/streaming_compression.c
  while (true) {
    const { value, done } = await reader.read();
    if (done) break;

    // TODO: stream-compress the chunks
    await writer.write(value);
  }

  await writer.close();

  // Free the zstd context and buffers
  if (zstdInBuff !== null) zstd._free(zstdInBuff);
  if (zstdOutBuff !== null) zstd._free(zstdOutBuff);
  if (cctx !== null) zstd.freeCCtx(cctx);
}

/*
 Handle the client request for a dictionary
*/
async function fetchDictionary(env, url) {
  // Just pass the request through to the assets fetch
  url.pathname = '/' + currentDictionary + '.dat';
  let asset = await env.ASSETS.fetch(url);
  return new Response(asset.body, {
    headers: {
      "content-type": "text/plain; charset=UTF-8",  /* Can be anything but text/plain will allow for Cloudflare to apply compression */
      "cache-control": "public, max-age=" + dictionaryExpiration,
      "use-as-dictionary": match
    }
  });
}

/*
 See if the client advertized a matching dictionary and the appropriate encoding
*/
function supportsCompression(request) {
  let hasDictionary = false;
  const availableDictionary = request.headers.get("available-dictionary");
  if (availableDictionary) {
    const availableHash = atob(availableDictionary.trim().replaceAll(':', ''));
    if (availableHash == currentHash) {
      hasDictionary = true;
    }
  }
  const supportsDCZ = request.cf.clientAcceptEncoding.indexOf("dcz") !== -1;
  return hasDictionary && supportsDCZ;
}

/*
  Make sure the dictionary is loaded and cached into the isolate global.
  The current implementation blocks all requests until the dictionary has been loaded.
  This can be modified to fail fast and only use dictionaries after they have loaded.
 */
async function dictionaryInit(request, env, ctx) {
  if (dictionaryJS === null && dictionaryLoaded === null) {
    let resolve;
    dictionaryLoaded = new Promise((res, rej) => {
      resolve = res;
    });
    // Keep the request alive until the dictionary loads
    ctx.waitUntil(dictionaryLoaded);
    const url = new URL(request.url);
    url.pathname = '/' + currentDictionary + '.dat';
    const response = await env.ASSETS.fetch(url);
    if (response.ok) {
      dictionaryJS = await response.arrayBuffer();
    }
    postInit();
    resolve(true);
  }
}

// wasm setup
async function zstdInit() {
  // we send our own instantiateWasm function
  // to the zstdlib module
  // so we can initialize the WASM instance ourselves
  // since Workers puts your wasm file in global scope
  // as a binding. In this case, this binding is called
  // `wasm` as that is the name Wrangler uses
  // for any uploaded wasm module
  if (zstd === null && zstdLoaded === null) {
    let resolve;
    zstdLoaded = new Promise((res, rej) => {
      resolve = res;
    });
    // Keep the request alive until zstd finished initializing
    zstd = await zstdlib({
      instantiateWasm(info, receive) {
        let instance = new WebAssembly.Instance(zstdwasm, info);
        receive(instance);
        return instance.exports;
      },
      locateFile(path, scriptDirectory) {
        // scriptDirectory is undefined, so this is a
        // no-op to avoid exception "TypeError: Invalid URL string."
        console.log("locateFile");
        return path
      },
    });
    postInit();
    resolve(true);
  }
}

function postInit() {
  if (!initialized) {
    if (zstd !== null && dictionaryJS !== null) {
      // copy the dictionary over to wasm
      try {
        let dict = zstd._malloc(dictionaryJS.byteLength)
        zstd.HEAPU8.set(dictionaryJS, dictionary);
        dictionary = zstd.createCDict(dict, dictionaryJS.byteLength, compressionLevel);
        if (dictionary === null) {
          console.log('Failed to create zstd dictionary');
        }
        zstd._free(dict);
        dictionaryJS = null;  // Free the java-side dictionary memory
        initialized = true;
      } catch (E) {
        console.log(E);
      }
    }
  }
}