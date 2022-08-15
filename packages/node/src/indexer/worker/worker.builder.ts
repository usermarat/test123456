// Copyright 2020-2021 OnFinality Limited authors & contributors
// SPDX-License-Identifier: Apache-2.0

import ChildProcess from 'child_process';
import * as workers from 'worker_threads';
import { Logger } from 'pino';
import { getLogger } from '../../utils/logger';

export type SerializableError = {
  message: string;
  stack?: string;
};

export type Request = {
  id: number | string;
  name: string;
  args: any[];
};

export type Response<T = any> = {
  id: number | string;
  error?: SerializableError;
  result?: T;
};

export type AsyncFunc<T = any> = (
  ...args: any[]
) => T | Promise<T | void> | void;
type AsyncMethods = Record<string, AsyncFunc>;

export function isMainProcess(): boolean {
  return !process.argv.includes('--workerId');
}

export function workerId(): number {
  const idx = process.argv.indexOf('--workerId');
  if (idx <= -1) return -1;

  return parseInt(process.argv[idx + 1], 10);
}

// TODO can we pass an event emitter rather than use parent port
// This would make it agnostic to child process

/* Builds an interface on the worker side to interact with main process */
export function registerWorker(fns: AsyncMethods): void {
  let functions: AsyncMethods;

  function register(fns: typeof functions) {
    functions = fns;
  }

  async function handleRequest(req: Request): Promise<void> {
    const fn = functions[req.name];

    if (!fn) {
      process.send(<Response>{
        id: req.id,
        error: {
          message: `handleRequest: Function "${req.name}" not found`,
        },
      });
      return;
    }

    try {
      const res = await fn(...req.args);

      process.send(<Response>{
        id: req.id,
        result: res,
      });
    } catch (e) {
      process.send(<Response>{
        id: req.id,
        error: e,
      });
    }
  }

  process.on('message', (req: Request) => {
    // console.log("worker ==>>", workerId(), req);
    void handleRequest(req);
  });

  register(fns);
}

/* Host side, used to initialise and interact with worker */
export class Worker<T extends AsyncMethods> {
  private worker: ChildProcess.ChildProcess;
  // private worker: workers.Worker;
  private logger: Logger;

  private responseListeners: Record<
    number | string,
    (data?: any, error?: SerializableError) => void
  > = {};

  private _reqCounter = 0;

  private constructor(path: string, fns: (keyof T)[], id: number) {
    this.worker = ChildProcess.fork(path, [
      ...process.argv,
      '--workerId',
      id.toString(),
    ]);
    // this.worker = new workers.Worker(path, {
    //   argv: process.argv,
    // });

    this.logger = getLogger(`worker: ${this.worker.pid}`);
    // this.logger = getLogger(`worker: ${this.worker.threadId}`);

    this.worker.on('message', (res: Response) => {
      // console.log('Worker <<==', id, res)
      if (this.responseListeners[res.id]) {
        this.responseListeners[res.id](res.result, res.error);

        delete this.responseListeners[res.id];
      } else {
        this.logger.warn(`No handler found for request: "${res.id}"`);
      }
    });

    this.worker.on('error', (error) => {
      this.logger.error(error, 'Worker error');
    });

    this.worker.on('messageerror', (error) => {
      this.logger.error(error, 'Worker message error');
    });

    this.worker.on('exit', (code) => {
      this.logger.error(`Worker exited with code ${code}`);
      process.exit(code);
    });

    // Add expected methods to class
    fns.map((fn: string) => {
      if (this[fn]) {
        throw new Error(`Method ${fn} is already defined`);
      }
      Object.assign(this, { [fn]: (...args: any[]) => this.execute(fn, args) });
    });
  }

  static create<T extends AsyncMethods>(
    path: string,
    fns: (keyof T)[],
    id: number,
  ): Worker<T> & T {
    const worker = new Worker(path, fns, id);

    return worker as Worker<T> & T;
  }

  private getReqId(): number {
    return this._reqCounter++;
  }

  // eslint-disable-next-line @typescript-eslint/require-await
  async terminate(): Promise<number> {
    this.worker.kill(0);
    return 0;
    // return this.worker.terminate();
  }

  private async execute<T>(fnName: string, ...args: any[]): Promise<T> {
    const id = this.getReqId();

    return new Promise<T>((resolve, reject) => {
      this.responseListeners[id] = (data, error) => {
        if (error) {
          const e = new Error(error.message);
          e.stack = error.stack ?? e.stack;
          reject(e);
        } else {
          resolve(data);
        }
      };

      this.worker.send(<Request>{
        id,
        name: fnName,
        args,
      });

      // this.worker.postMessage(<Request>{
      //   id,
      //   name: fnName,
      //   args,
      // });
    });
  }
}
