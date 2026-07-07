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

  showBackButton = false;

  private bibleData: any;

  constructor(
    private bibleApi: BibleApi
  ) {}

  ngOnInit(): void {

    console.log('starting ...');

    this.bibleApi.getHits().subscribe(data => {

      this.bibleData = data;

      console.log(data);

      this.renderChart(data);

    });
  }

  renderChart(data: any): void {

    const books: { name: string; hits: number }[] =
      Object.values(data).map((book: any) => ({
        name: String(book.bookName),
        hits: Number(book.book_verse_hit_total)
      }));

    this.renderHorizontalChart(books);
  }


  renderHorizontalChart(
  items: { name: string; hits: number }[]
): void {

  d3.select('#chart')
    .selectAll('*')
    .remove();

  const width = 1200;
  const height = items.length * 30;

  const svg = d3.select('#chart')
    .append('svg')
    .attr('width', width)
    .attr('height', height);

  const x = d3.scaleLinear()
    .domain([
      0,
      d3.max(items, d => d.hits) || 0
    ])
    .nice()
    .range([0, width - 250]);

  const y = d3.scaleBand()
    .domain(items.map(d => d.name))
    .range([20, height - 20])
    .padding(0.1);

  svg.selectAll('rect')
    .data(items)
    .enter()
    .append('rect')
    .attr('x', 220)
    .attr('y', d => y(d.name)!)
    .attr('width', d => x(d.hits))
    .attr('height', y.bandwidth())
    .attr('fill', 'steelblue')
    .attr('cursor', 'pointer')
    .on('click', (_event, d) => {

      console.log('Clicked:', d.name);

      this.renderChapterChart(d.name);

    })
    .append('title')
    .text(d =>
      `${d.name}: ${d.hits.toLocaleString()}`
    );

  svg.append('g')
    .attr('transform', 'translate(220,0)')
    .call(d3.axisLeft(y));

  svg.append('g')
    .attr('transform', `translate(220,${height - 20})`)
    .call(d3.axisBottom(x));
}

  renderChapterChart(bookName: string): void {

    const book = this.bibleData[bookName];

    const chapters = Object.entries(book.chapter_verse_hit_total)
      .map(([chapter, hits]) => ({
        name: chapter,
        hits: Number(hits)
      }));

    //console.log(chapters);
    this.renderHorizontalChart(chapters);

  }
}
