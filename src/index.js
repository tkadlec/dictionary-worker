import zstdlib from "../zstd-wasm-compress/bin/zstdlib.js";
import zstdwasm from "../zstd-wasm-compress/bin/zstdlib.wasm";

// File name of the current dictionary asset (TODO: see if there is a way to get this dynamically)
const currentDictionary = "Yu5Mly7MyX8QiEl-NuhjlRgy-FdDzBNqDYXSygasUyc";

// Psuedo-path where the dictionaries will be served from (shouldn't collide with a real directory)
const dictionaryPath = "/dictionary/";

// Dictionary options
const match = 'match="/*.js", match-dest="script"'; // Match pattern for the URLs to be compressed
const dictionaryExpiration = 30 * 24 * 3600;                 // 30 day expiration on the dictionary itself

// Compression options
const blocking = true;   // Block requests until wasm and the dictionary have loaded
const compressionLevel = 10;
const compressionWindowLog = 20;  // Compression window should be at least as long as the dictionary + typical response - 2 ^ 20 = 1MB

// Internal globals for managing state while waiting for the dictionary and zstd wasm to load
let zstd = null;
let dictionaryLoaded = null;
let zstdLoaded = null;
let initialized = false;
let dictionary = null;
let dictionarySize = 0;
let dictionaryJS = null;
const currentHash = atob(currentDictionary.replaceAll('-', '+').replaceAll('_', '/'));
const dictionaryPathname = dictionaryPath + currentDictionary + '.dat';
const dczHeader = new Uint8Array([0x5e, 0x2a, 0x4d, 0x18, 0x20, 0x00, 0x00, 0x00, ...Uint8Array.from(currentHash, c => c.charCodeAt(0))]);

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
    const host = request.headers.get("x-host") || new URL(request.url).hostname;
    
    if (!host) {
      return await fetch(request);
    }

    const url = new URL(request.url);
    if (!url.pathname.endsWith('.js') && !url.pathname.endsWith('.dat')) {
      // Create a new request with the origin host
      const originUrl = new URL(request.url);
      originUrl.host = host;
      return await fetch(originUrl.toString(), request);
    }

    // Initialize both in parallel
    const [dictPromise, zstdPromise] = await Promise.all([
      dictionaryInit(request, env, ctx),
      zstdInit()
    ]).catch(e => {
      console.error("Initialization error:", e);
      return [null, null];
    });

    // Don't modify the host if it's already different from the x-host
    if (new URL(request.url).hostname === host) {
      url.host = host;
    }
    console.log('originURL: ' + url.toString())

    const isDictionary = url.pathname == dictionaryPathname;
    if (isDictionary) {
      return await fetchDictionary(env, url);
    } else {
      console.log('origin URL: ' + url.toString())
      const originUrl = new URL(request.url);
      originUrl.host = host;
      console.log('origin URL: ' + originUrl.toString())
      const original = await fetch(originUrl.toString(), request);

      // Check if the URL ends with .js regardless of the host
      if (original.ok && url.pathname.endsWith('.js')) {
        // block on the dictionary/zstd init if necessary
        if (blocking) {
          if (zstd === null) { await zstdLoaded; }
          if (dictionary === null) { await dictionaryLoaded; }
        }
        console.log('supports compression ' + supportsCompression(request));
        console.log('zstd', zstd);
        console.log('dictionary', dictionary);

        if (supportsCompression(request) && zstd !== null && dictionary !== null) {
          console.log('yup...supports compression')
          return await compressResponse(original, ctx);
        } else {
          console.log("nope...not gonna compress for some reason")
          const response = new Response(original.body, original);
          response.headers.append("Link", '<' + dictionaryPathname + '>; rel="compression-dictionary"',);
          return response;
        }
      }
      // Return the original response for non-js files
      return original;
    }
  }
}

/*
  Dictionary-compress the response
*/
async function compressResponse(original, ctx) {
  const { readable, writable } = new TransformStream();
  ctx.waitUntil(compressStream(original.body, writable));

  // Add the appropriate headers
  const response = new Response(readable, original);
  let ver = zstd.versionNumber();
  response.headers.append("Link", '<' + dictionaryPathname + '>; rel="compression-dictionary"',);
  response.headers.set("X-Zstd-Version", ver);
  response.headers.set("Vary", 'Accept-Encoding, Available-Dictionary',);
  response.headers.set("Content-Encoding", 'dcz',);
  return response;
}

async function compressStream(readable, writable) {
    const reader = readable.getReader();
    const writer = writable.getWriter();
    
    // Add timeout handling
    const timeout = setTimeout(() => {
        reader.cancel();
        writer.abort(new Error('Compression timed out'));
    }, 25000); // 25 second timeout

    try {
        // Setup compression context
        const cctx = zstd?.createCCtx();
        if (!cctx) throw new Error('Failed to create compression context');

        const inSize = zstd.CStreamInSize();
        const outSize = zstd.CStreamOutSize();
        const zstdInBuff = zstd._malloc(inSize);
        const zstdOutBuff = zstd._malloc(outSize);

        // configure the zstd parameters
        zstd.CCtx_setParameter(cctx, zstd.cParameter.c_compressionLevel, compressionLevel);
        zstd.CCtx_setParameter(cctx, zstd.cParameter.c_windowLog, compressionWindowLog );
        
        zstd.CCtx_refCDict(cctx, dictionary);

        // write the dcz header
        await writer.write(dczHeader);
        
        let isFirstChunk = true;
        let chunksGathered = 0;

        // streaming compression modeled after https://github.com/facebook/zstd/blob/dev/examples/streaming_compression.c
        while (true) {
            const { value, done } = await reader.read();
            const size = done ? 0 : value.byteLength;

            // Grab chunks of the input stream in case it is bigger than the zstd buffer
            let pos = 0;
            while (pos < size || done) {
                const endPos = Math.min(pos + inSize, size);
                const chunkSize = done ? 0 : endPos - pos;
                const chunk = done ? null : value.subarray(pos, endPos);
                pos = endPos;

                try {
                    if (chunkSize > 0) {
                        zstd.HEAPU8.set(chunk, zstdInBuff);
                    }

                    const inBuffer = new zstd.inBuffer();
                    inBuffer.src = zstdInBuff;
                    inBuffer.size = chunkSize;
                    inBuffer.pos = 0;
                    let finished = false;
                    do {
                        const outBuffer = new zstd.outBuffer();
                        outBuffer.dst = zstdOutBuff;
                        outBuffer.size = outSize;
                        outBuffer.pos = 0;

                        // Use a naive flushing strategy for now. Flush the first chunk immediately and then let zstd decide
                        // when each chunk should be emitted (likey accumulate until complete).
                        // Also, every 5 chunks that were gathered, flush irregardless.
                        let mode = zstd.EndDirective.e_continue;
                        if (done) {
                            mode = zstd.EndDirective.e_end;
                        } else if (isFirstChunk || chunksGathered >= 4) {
                            mode = zstd.EndDirective.e_flush;
                            isFirstChunk = false;
                            chunksGathered = 0;
                        }

                        // Keep track of the number of chunks processed where we didn't send any response.
                        if (outBuffer.pos == 0) chunksGathered++;

                        const remaining = zstd.compressStream2(cctx, outBuffer, inBuffer, mode);

                        if (outBuffer.pos > 0) {
                            const data = new Uint8Array(zstd.HEAPU8.buffer, outBuffer.dst, outBuffer.pos);
                            await writer.write(data);
                        }

                        finished = done ? (remaining == 0) : (inBuffer.pos == inBuffer.size);
                    } while (!finished);
                } catch (E) {
                    console.log(E);
                }
                if (done) break;
            }
            if (done) break;
        }

        await writer.close();

        // Free the zstd context and buffers
        if (zstdInBuff !== null) zstd._free(zstdInBuff);
        if (zstdOutBuff !== null) zstd._free(zstdOutBuff);
        if (cctx !== null) zstd.freeCCtx(cctx);
    } catch (error) {
        console.error('Compression error:', error);
        throw error;
    } finally {
        clearTimeout(timeout);
    }
}

/*
 Handle the client request for a dictionary
*/
async function fetchDictionary(env, url) {
  console.log('fetching dictionary');
  console.log('dcitionary url: ' + url);
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
        dictionaryLoaded = Promise.resolve().then(async () => {
            const url = new URL(request.url);
            url.pathname = '/' + currentDictionary + '.dat';
            const response = await env.ASSETS.fetch(url);
            if (!response.ok) {
                throw new Error('Failed to load dictionary');
            }
            dictionaryJS = await response.bytes();
            postInit();
            return true;
        });
    }
    return dictionaryLoaded;
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
    }).catch(E => console.log(E));
    postInit();
    resolve(true);
  }
}

// After both the dictionary and wasm have initialized, prepare the dictionary into zstd
// memory so it can be reused efficiently.
function postInit() {
  if (!initialized) {
    if (zstd !== null && dictionaryJS !== null) {
      // copy the dictionary over to wasm
      try {
        let d = zstd._malloc(dictionaryJS.byteLength)
        dictionarySize = dictionaryJS.byteLength;
        zstd.HEAPU8.set(dictionaryJS, d);
        dictionaryJS = null;
        dictionary = zstd.createCDict_byReference(d, dictionarySize, compressionLevel);
        initialized = true;
      } catch (E) {
        console.log(E);
      }
    }
  }
}