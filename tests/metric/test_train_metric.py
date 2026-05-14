# Copyright (c) ModelScope Contributors. All rights reserved.
import torch

from twinkle.metric.train_metric import TrainMetric


def test_train_metric_reports_tokens_from_outputs():
    metric = TrainMetric(device_mesh=None, process_group=None)

    metric.accumulate(
        inputs={'labels': torch.tensor([[1, -100, 2]])},
        outputs={'num_tokens': torch.tensor(5)},
        lr=[1e-4],
        step=2,
        gradient_accumulation_steps=1,
    )

    results = metric.calculate()

    assert results['interval tokens'] == 5
    assert results['total tokens'] == 5
    assert 'tokens/s' in results


def test_train_metric_counts_label_tokens_as_fallback():
    metric = TrainMetric(device_mesh=None, process_group=None)

    metric.accumulate(
        inputs={'labels': torch.tensor([[1, -100, 2], [-100, 3, 4]])},
        outputs={},
        lr=[1e-4],
        step=2,
        gradient_accumulation_steps=1,
    )

    results = metric.calculate()

    assert results['interval tokens'] == 4
    assert results['total tokens'] == 4
    assert 'tokens/s' in results
