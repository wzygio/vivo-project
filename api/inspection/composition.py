"""Headless production bindings; domain application services retain all rules."""
from datetime import date
from functools import partial

from api.inspection.application import InspectionJob


def produce_yield(product: str, reference_date: date) -> list[dict]:
    from api.inspection.yield_publication import publish_product
    # Original producer derives its normal bounded baseline from the current day.
    # Historical rebuilds remain the explicit per-domain maintenance command.
    if reference_date != date.today():
        raise ValueError('Scheduled Yield production requires current report date')
    result = publish_product(product, kinds=('trend', 'lot'))
    return [{'module': 'yield', 'product': product,
             'status': 'success' if result.get('status') == 'success' else 'failed'}]


def produce_qtime(products: tuple[str, ...], reference_date: date) -> list[dict]:
    from src.indicator_domain.composition import build_qtime_service, build_qtime_alert_results
    from src.indicator_domain.application.qtime.cached_monitoring import publish_refreshed_qtime_alerts
    from src.shared_kernel.infrastructure.db_handler import DatabaseManager
    service = build_qtime_service(DatabaseManager())
    refreshes = service.refresh_snapshots(as_of=reference_date)
    if not refreshes or not all(row.refreshed_from_database for row in refreshes):
        raise ValueError('Q-Time source refresh did not complete')
    if not publish_refreshed_qtime_alerts(service, refreshes, as_of=reference_date):
        raise ValueError('Q-Time publication failed')
    reader = build_qtime_alert_results()
    for product in products:
        result = reader.read(product, reference_date)
        if result.get('state') not in ('ok', 'alert') or result.get('release_evidence') is not True:
            raise ValueError('Q-Time publication verification failed')
    return [{'module': 'qtime', 'status': 'success'}]


def build_jobs(products: tuple[str, ...], reference_date: date) -> list[InspectionJob]:
    from api.inspection.producers_inline import produce_inline
    from api.inspection.producers_auxiliary import produce_auxiliary
    jobs = [InspectionJob('inline:'+product,
                          tuple((module, product) for module in ('spc','ctq','aoi_tt','aoi_rs')),
                          partial(produce_inline, product, reference_date)) for product in products]
    jobs.extend(InspectionJob('yield:'+product, (('yield',product),),
                              partial(produce_yield, product, reference_date)) for product in products)
    jobs.append(InspectionJob('qtime', (('qtime',''),), partial(produce_qtime, products, reference_date)))
    jobs.append(InspectionJob('auxiliary', tuple((module,'') for module in ('ijp_aa','ijp_hole','parts','iqc_evaporation')),
                              partial(produce_auxiliary, products, reference_date)))
    return jobs
