import { TestBed } from '@angular/core/testing';

import { BibleApi } from './bible-api';

describe('BibleApi', () => {
  let service: BibleApi;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(BibleApi);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
