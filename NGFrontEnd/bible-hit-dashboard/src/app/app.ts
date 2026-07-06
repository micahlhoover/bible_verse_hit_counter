import { Component, signal, OnInit } from '@angular/core';
import { RouterOutlet } from '@angular/router';
import { BibleApi } from './services/bible-api';
import * as d3 from 'd3';

@Component({
  selector: 'app-root',
  imports: [RouterOutlet],
  templateUrl: './app.html',
  styleUrl: './app.css'
})
export class App implements OnInit {

  protected readonly title = signal('bible-hit-dashboard');

  constructor(
    private bibleApi: BibleApi
  ) {}

  ngOnInit(): void {

    console.log('starting ...');

    this.bibleApi.getHits().subscribe(data => {

      console.log('updating on init');
      console.log(data);

      this.renderChart(data);

    });
  }

  renderChart(data: any): void {

    console.log('Rendering chart');

    // we'll put D3 code here

  }
}
