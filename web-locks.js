'use strict';

const { EventEmitter } = require('events');
const threads = require('worker_threads');
const { isMainThread, parentPort } = threads;
const isWorkerThread = !isMainThread;

const LOCKED = 0;
const UNLOCKED = 1;
const DEFAULT_CALL = 0;
const DIRECT_CALL = 1;

let locks = null; // LockManager instance

class Lock {
  constructor(name, mode = 'exclusive', buffer = null) {
    this.name = name;
    this.mode = mode; // 'exclusive' or 'shared'
    this.queue = [];
    this.owner = false;
    this.trying = false;
    this.buffer = buffer ? buffer : new SharedArrayBuffer(4);
    this.flag = new Int32Array(this.buffer, 0, 1);
    if (!buffer) Atomics.store(this.flag, 0, UNLOCKED);
  }

  enter(handler, signal) {
    return new Promise((resolve, reject) => {
      const task = {
        handler,
        signal,
        resolve,
        reject,
        rejected: false,
        entered: false,
      };
      this.queue.push(task);
      this.trying = true;
      signal.addEventListener(
        'abort',
        () => {
          if (!task.entered) {
            task.rejected = true;
            reject(new Error(`AbortError: ${signal.reason}`));
          }
        },
        { once: true }
      );
      setTimeout(() => {
        this.tryEnter();
      }, 0);
    });
  }

  tryEnter() {
    if (this.queue.length === 0) return;
    const prev = Atomics.exchange(this.flag, 0, LOCKED);
    if (prev === LOCKED) return;
    this.owner = true;
    this.trying = false;
    const task = this.queue.shift();
    task.entered = true;
    const { handler, signal, resolve, reject, rejected } = task;
    if (signal.aborted) {
      this.leave();
      if (!rejected) {
        reject(new Error(`AbortError: ${signal.reason}`));
      }
      return;
    }
    handler(this)
      .then((res) => {
        this.leave();
        resolve(res);
      })
      .catch((err) => {
        this.leave();
        reject(err);
      });
  }

  leave() {
    if (!this.owner) return;
    Atomics.store(this.flag, 0, UNLOCKED);
    this.owner = false;
    const message = { webLocks: true, kind: 'leave', name: this.name };
    locks.send(message);
    this.tryEnter();
  }

  tryDirect(handler) {
    return new Promise((resolve, reject) => {
      if (this.queue.length !== 0) {
        handler(null).then(resolve).catch(reject);
      }
      const prev = Atomics.exchange(this.flag, 0, LOCKED);
      if (prev === LOCKED) handler(null).then(resolve).catch(reject);
      this.owner = true;
      handler(this)
        .then((res) => {
          this.leave();
          resolve(res);
        })
        .catch((err) => {
          this.leave();
          reject(err);
        });
    });
  }
}

class LockManagerSnapshot {
  constructor(resources) {
    const held = [];
    const pending = [];
    this.held = held;
    this.pending = pending;

    for (const lock of resources) {
      if (lock.queue.length > 0) {
        pending.push(...lock.queue);
      }
      if (lock.owner) {
        held.push(lock);
      }
    }
  }
}

class LockManager {
  constructor() {
    this.collection = new Map();
    this.workers = new Set();
    if (isWorkerThread) {
      parentPort.on('message', (message) => {
        this.receive(message);
      });
    }
  }

  async request(name, options, handler) {
    if (typeof options === 'function') {
      handler = options;
      options = {};
    }
    const {
      mode = 'exclusive',
      signal = null,
      ifAvailable = false,
      steal = false,
    } = options;

    if (name[0] === '-') throw new Error('NotSupportedError');
    if (steal === true && ifAvailable === true)
      throw new Error('NotSupportedError');
    if (steal === true && mode !== 'exclusive')
      throw new Error('NotSupportedError');
    if (signal && (steal === true || ifAvailable === true))
      throw new Error('NotSupportedError');

    let lock = this.collection.get(name);
    if (!lock) {
      lock = new Lock(name, mode);
      this.collection.set(name, lock);
      const { buffer } = lock;
      const message = { webLocks: true, kind: 'create', name, mode, buffer };
      locks.send(message);
    } else if (lock.mode !== mode) throw new Error('The mode can`t be changed');

    if (signal && signal.aborted) {
      throw new Error(`AbortError: ${signal.reason}`);
    }

    let type = DEFAULT_CALL;
    if (ifAvailable) type = DIRECT_CALL;

    const func = [
      async () => lock.enter(handler, signal),
      async () => lock.tryDirect(handler),
    ];

    const result = await func[type]();

    return result;
  }

  query() {
    const snapshot = new LockManagerSnapshot();
    return Promise.resolve(snapshot);
  }

  attach(worker) {
    this.workers.add(worker);
    worker.on('message', (message) => {
      for (const peer of this.workers) {
        if (peer !== worker) {
          peer.postMessage(message);
        }
      }
      this.receive(message);
    });
  }

  send(message) {
    if (isWorkerThread) {
      parentPort.postMessage(message);
      return;
    }
    for (const worker of this.workers) {
      worker.postMessage(message);
    }
  }

  receive(message) {
    if (!message.webLocks) return;
    const { kind, name, mode, buffer } = message;
    if (kind === 'create') {
      const lock = new Lock(name, mode, buffer);
      this.collection.set(name, lock);
      return;
    }
    if (kind === 'leave') {
      for (const lock of this.collection.values()) {
        if (lock.name === name && lock.trying) {
          lock.tryEnter();
        }
      }
    }
  }
}

class AbortError extends Error {
  constructor(message) {
    super(message);
    this.name = 'AbortError';
  }
}

class AbortSignal extends EventEmitter {
  constructor() {
    super();
    this.aborted = false;
    this.on('abort', () => {
      this.aborted = true;
    });
  }

  addEventListener(type, listener, options) {
    const { once = false } = options;
    if (once) this.once(type, listener);
    else this.on(type, listener);
  }
}

class AbortController {
  constructor() {
    this.signal = new AbortSignal();
  }

  abort() {
    const error = new AbortError('The request was aborted');
    this.signal.emit('abort', error);
  }
}

locks = new LockManager();

module.exports = { locks, AbortController };
