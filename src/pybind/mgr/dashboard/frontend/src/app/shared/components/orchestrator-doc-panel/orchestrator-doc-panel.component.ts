import { Component, OnInit } from '@angular/core';

import { DocUrls } from '../../../shared/constants/app.constants';
import { CephReleaseNamePipe } from '../../pipes/ceph-release-name.pipe';
import { SummaryService } from '../../services/summary.service';

@Component({
  selector: 'cd-orchestrator-doc-panel',
  templateUrl: './orchestrator-doc-panel.component.html',
  styleUrls: ['./orchestrator-doc-panel.component.scss']
})
export class OrchestratorDocPanelComponent implements OnInit {
  docsUrl: string;

  constructor(
    private cephReleaseNamePipe: CephReleaseNamePipe,
    private summaryService: SummaryService
  ) {}

  ngOnInit() {
    const subs = this.summaryService.subscribe((summary: any) => {
      if (!summary) {
        return;
      }
      // @ts-ignore
      const releaseName = this.cephReleaseNamePipe.transform(summary.version);
      this.docsUrl = DocUrls.orchestrator;

      setTimeout(() => {
        subs.unsubscribe();
      }, 0);
    });
  }
}
