import { Injectable } from '@angular/core';

import { BehaviorSubject, Subscription } from 'rxjs';
import { filter, first, map } from 'rxjs/operators';
import { CephReleaseNamePipe } from '../pipes/ceph-release-name.pipe';
import { SummaryService } from './summary.service';

@Injectable({
  providedIn: 'root'
})
export class DocService {
  private releaseDataSource = new BehaviorSubject<string>('5');
  releaseData$ = this.releaseDataSource.asObservable();

  constructor(
    private summaryservice: SummaryService,
    private cephReleaseNamePipe: CephReleaseNamePipe
  ) {
    this.summaryservice.subscribeOnce((summary) => {
      const releaseName = this.cephReleaseNamePipe.transform(summary.version);
      this.releaseDataSource.next(releaseName);
    });
  }

  urlGenerator(section: string, release = 'master'): string {
    const docVersion = release === 'master' ? 'latest' : release;
    const domain = `https://access.redhat.com/documentation/en-us/red_hat_ceph_storage/${docVersion}/html/`;
    const domainCeph = `https://ceph.io/`;

    const sections = {
      iscsi: `${domain}dashboard_guide/block_devices#iscsi-functions`,
      prometheus: `${domain}dashboard_guide/managing-the-cluster#viewing-and-managing-alerts`,
      'nfs-ganesha': `${domain}dashboard_guide/object-gateway#nfs-ganesha`,
      'rgw-nfs': `${domain}object_gateway_configuration_and_administration_guide/rgw-configuration-rgw#exporting-the-namespace-to-nfs-ganesha-rgw`,
      rgw: `${domain}dashboard_guide/object-gateway`,
      dashboard: `${domain}dashboard_guide/`,
      grafana: `${domain}dashboard_guide/managing-the-cluster#managing-the-prometheus-environment_dash`,
      orch: `${domain}operations_guide/orchestrator/`,
      pgs: `https://access.redhat.com/labs/cephpgc/`,
      help: `${domainCeph}help/`,
      security: `${domainCeph}security/`,
      trademarks: `${domainCeph}legal-page/trademarks/`,
      'dashboard-landing-page-status': `${domain}mgr/dashboard/#dashboard-landing-page-status`,
      'dashboard-landing-page-performance': `${domain}mgr/dashboard/#dashboard-landing-page-performance`,
      'dashboard-landing-page-capacity': `${domain}mgr/dashboard/#dashboard-landing-page-capacity`
    };

    return sections[section];
  }

  subscribeOnce(
    section: string,
    next: (release: string) => void,
    error?: (error: any) => void
  ): Subscription {
    return this.releaseData$
      .pipe(
        filter((value) => !!value),
        map((release) => this.urlGenerator(section, release)),
        first()
      )
      .subscribe(next, error);
  }
}
