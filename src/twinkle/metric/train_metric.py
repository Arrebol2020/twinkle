# Copyright (c) ModelScope Contributors. All rights reserved.
import time
from typing import List, Union

from ..data_format import InputFeature, ModelOutput
from .base import Metric


class TrainMetric(Metric):
    """The training metric.

    Args:
        device_mesh: The device mesh
        process_group: The process group to collect data from
    """

    def __init__(self, device_mesh=None, process_group=None, **kwargs):
        super().__init__(device_mesh, process_group, **kwargs)
        self.lr = None
        self.step = 0
        self.last_step = 0
        self.gradient_accumulation_steps = 1
        self.start_time = time.time()
        self.time = time.time()
        self.lrs = []
        self.total_tokens = 0
        self.interval_tokens = 0

    def accumulate(self, inputs: Union[InputFeature, List[InputFeature]], outputs: ModelOutput, **kwargs):
        lr = kwargs.get('lr')
        if isinstance(lr, list):
            lr = [f'{x:.6e}' for x in lr]
        else:
            lr = f'{lr:.6e}'
        self.lr = lr
        self.lrs.append(lr)
        self.step = kwargs.get('step')
        self.gradient_accumulation_steps = kwargs.get('gradient_accumulation_steps', self.gradient_accumulation_steps)
        num_tokens = self._get_num_tokens(inputs, outputs)
        self.total_tokens += num_tokens
        self.interval_tokens += num_tokens

    @staticmethod
    def _as_int(value):
        if value is None:
            return 0
        if hasattr(value, 'detach'):
            value = value.detach()
        if hasattr(value, 'sum'):
            value = value.sum()
        if hasattr(value, 'item'):
            value = value.item()
        return int(value)

    @classmethod
    def _count_label_tokens(cls, inputs):
        if inputs is None:
            return 0
        if isinstance(inputs, dict):
            labels = inputs.get('labels')
            if labels is None:
                return 0
            return cls._as_int(labels != -100)
        if isinstance(inputs, (list, tuple)):
            return sum(cls._count_label_tokens(item) for item in inputs)
        return 0

    @classmethod
    def _get_num_tokens(cls, inputs, outputs):
        if isinstance(outputs, dict) and outputs.get('num_tokens') is not None:
            return cls._as_int(outputs.get('num_tokens'))
        return cls._count_label_tokens(inputs)

    def reset(self):
        self.time = time.time()
        self.last_step = self.step
        self.interval_tokens = 0

    def calculate(self):
        results = {}
        if self.lr is not None:
            if isinstance(self.lr, list) and len(self.lr) == 1:
                self.lr = self.lr[0]
            if isinstance(self.lr, list):
                for idx, lr in enumerate(self.lr):
                    results[f'learning rate(param group {idx + 1})'] = lr
            else:
                results['learning rate'] = self.lr
        if self.step is not None:
            results['iters'] = self.step // self.gradient_accumulation_steps
            interval = time.time() - self.time
            speed = (self.step - self.last_step) / interval / self.gradient_accumulation_steps
            if interval < 60:
                results['total time elapse'] = f'{(time.time() - self.start_time):.0f} seconds'
            else:
                results['total time elapse'] = f'{(time.time() - self.start_time) / 60:.1f} minutes'
            results['speed'] = f'{speed:.2f} iters/s'
            total_interval_tokens = sum(self.gather_results([self.interval_tokens]))
            total_tokens = sum(self.gather_results([self.total_tokens]))
            results['tokens/s'] = f'{total_interval_tokens / interval:.2f}'
            results['interval tokens'] = total_interval_tokens
            results['total tokens'] = total_tokens
        self.reset()
        return results
