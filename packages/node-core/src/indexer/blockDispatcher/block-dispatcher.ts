// Copyright 2020-2023 SubQuery Pte Ltd authors & contributors
// SPDX-License-Identifier: GPL-3.0

import {getHeapStatistics} from 'v8';
import {OnApplicationShutdown} from '@nestjs/common';
import {EventEmitter2} from '@nestjs/event-emitter';
import {last} from 'lodash';
import {Observable, Subscriber, mergeMap} from 'rxjs';
import {NodeConfig} from '../../configure';
import {IndexerEvent} from '../../events';
import {getLogger} from '../../logger';
import {profilerWrap} from '../../profiler';
import {Queue, AutoQueue, /*AutoQueue2,*/ delay, memoryLock, waitForBatchSize} from '../../utils';
import {DynamicDsService} from '../dynamic-ds.service';
import {PoiService} from '../poi/poi.service';
import {SmartBatchService} from '../smartBatch.service';
import {StoreService} from '../store.service';
import {StoreCacheService} from '../storeCache';
import {IProjectNetworkConfig, IProjectService, ISubqueryProject} from '../types';
import {BaseBlockDispatcher, ProcessBlockResponse} from './base-block-dispatcher';

const logger = getLogger('BlockDispatcherService');

type BatchBlockFetcher<B> = (heights: number[]) => Promise<B[]>;

/**
 * @description Intended to behave the same as WorkerBlockDispatcherService but doesn't use worker threads or any parallel processing
 */
export abstract class BlockDispatcher<B, DS>
  extends BaseBlockDispatcher<Queue<number>, DS>
  implements OnApplicationShutdown
{
  private processQueue: AutoQueue<void>;

  private fetchBlocksBatches: BatchBlockFetcher<B>;

  private fetching = false;
  private isShutdown = false;

  protected abstract indexBlock(block: B): Promise<ProcessBlockResponse>;
  protected abstract getBlockHeight(block: B): number;

  private queueX: AutoQueue<any>;

  constructor(
    nodeConfig: NodeConfig,
    eventEmitter: EventEmitter2,
    projectService: IProjectService<DS>,
    smartBatchService: SmartBatchService,
    storeService: StoreService,
    storeCacheService: StoreCacheService,
    poiService: PoiService,
    project: ISubqueryProject<IProjectNetworkConfig>,
    dynamicDsService: DynamicDsService<DS>,
    fetchBlocksBatches: BatchBlockFetcher<B>
  ) {
    super(
      nodeConfig,
      eventEmitter,
      project,
      projectService,
      new Queue(nodeConfig.batchSize * 3),
      smartBatchService,
      storeService,
      storeCacheService,
      poiService,
      dynamicDsService
    );
    this.processQueue = new AutoQueue(nodeConfig.batchSize * 3);
    this.queueX = new AutoQueue(nodeConfig.batchSize * 2, nodeConfig.batchSize);

    if (this.nodeConfig.profiler) {
      this.fetchBlocksBatches = profilerWrap(fetchBlocksBatches, 'BlockDispatcher', 'fetchBlocksBatches');
    } else {
      this.fetchBlocksBatches = fetchBlocksBatches;
    }
  }

  onApplicationShutdown(): void {
    this.isShutdown = true;
    this.processQueue.abort();
  }

  enqueueBlocks(heights: number[], latestBufferHeight?: number): void {
    // In the case where factors of batchSize is equal to bypassBlock or when heights is []
    // to ensure block is bypassed, we set the latestBufferHeight to the heights
    // make sure lastProcessedHeight in metadata is updated
    if (!!latestBufferHeight && !heights.length) {
      heights = [latestBufferHeight];
    }
    logger.info(`Enqueueing blocks ${heights[0]}...${last(heights)}, total ${heights.length} blocks`);

    this.queue.putMany(heights);

    this.latestBufferedHeight = latestBufferHeight ?? last(heights) ?? this.latestBufferedHeight;
    void this.fetchBlocksFromQueue();
  }

  flushQueue(height: number): void {
    super.flushQueue(height);
    this.processQueue.flush();
  }

  private memoryleft(): number {
    return this.smartBatchService.heapMemoryLimit() - getHeapStatistics().used_heap_size;
  }

  private async fetchBlocksFromQueue(): Promise<void> {
    if (this.fetching || this.isShutdown) return;
    // Process queue is full, no point in fetching more blocks
    // if (this.processQueue.freeSpace < this.nodeConfig.batchSize) return;

    this.fetching = true;

    try {
      while (!this.isShutdown) {
        const blockNum = this.queue.take();

        // Used to compare before and after as a way to check if queue was flushed
        const bufferedHeight = this._latestBufferedHeight;

        // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
        if (!blockNum || !this.queueX.freeSpace! /* || (this.processQueue.freeSpace! - this.queueX.size!) <= 0*/) {
          // console.log('xxxx', this.processQueue.freeSpace!, this.queueX.size, this.queueX.runningTasks.length, Object.keys(this.queueX.outOfOrderTasks).length)
          await delay(1);
          continue;
        }

        // if (memoryLock.isLocked()) {
        //   await memoryLock.waitForUnlock();
        // }

        void this.queueX
          .put(async () => {
            const [block] = await this.fetchBlocksBatches([blockNum]);

            // this.smartBatchService.addToSizeBuffer([block]);
            return block;
          })
          .then((block) => {
            const height = this.getBlockHeight(block);
            // console.log('Indexing block', height)
            // console.log('xxxx', this.processQueue.freeSpace!, this.queueX.size)

            void this.processQueue.put(async () => {
              // Check if the queues have been flushed between queue.takeMany and fetchBlocksBatches resolving
              // Peeking the queue is because the latestBufferedHeight could have regrown since fetching block
              const peeked = this.queue.peek();
              if (bufferedHeight > this._latestBufferedHeight || (peeked && peeked < blockNum)) {
                logger.info(`Queue was reset for new DS, discarding fetched blocks`);
                return;
              }

              try {
                this.preProcessBlock(height);
                // Inject runtimeVersion here to enhance api.at preparation
                const processBlockResponse = await this.indexBlock(block);

                await this.postProcessBlock(height, processBlockResponse);

                //set block to null for garbage collection
                // (block as any) = null;
              } catch (e: any) {
                // TODO discard any cache changes from this block height
                if (this.isShutdown) {
                  return;
                }
                logger.error(
                  e,
                  `failed to index block at height ${height} ${e.handler ? `${e.handler}(${e.stack ?? ''})` : ''}`
                );
                throw e;
              }
            });
          });

        this.eventEmitter.emit(IndexerEvent.BlockQueueSize, {
          value: this.processQueue.size,
        });
      }
    } catch (e: any) {
      logger.error(e, 'Failed to fetch blocks from queue');
      if (!this.isShutdown) {
        process.exit(1);
      }
    } finally {
      this.fetching = false;
    }
  }
}
