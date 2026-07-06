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

    // const books = Object.values(data).map((book: any) => ({
    //   name: book.bookName,
    //   hits: Number(book.book_verse_hit_total)
    // }));

const books: { name: string; hits: number }[] =
  Object.values(data).map((book: any) => ({
    name: String(book.bookName),
    hits: Number(book.book_verse_hit_total)
  }));

    // const topBooks = books
    //   .sort((a, b) => b.hits - a.hits)
    //   .slice(0, 20);

    d3.select('#chart')
      .selectAll('*')
      .remove();

    const width = 1200;
    const height = 700;

    const svg = d3.select('#chart')
      .append('svg')
      .attr('width', width)
      .attr('height', height);

    const x = d3.scaleBand()
      .domain(books.map(d => d.name))
      .range([80, width - 20])
      .padding(0.1);

    const y = d3.scaleLinear()
      .domain([
        0,
        d3.max(books, d => d.hits) || 0
      ])
      .nice()
      .range([height - 80, 20]);

    svg.selectAll('rect')
      .data(books)
      .enter()
      .append('rect')
      .attr('x', d => x(d.name)!)
      .attr('y', d => y(d.hits))
      .attr('width', x.bandwidth())
      .attr('height', d => height - 80 - y(d.hits))
      .attr('fill', 'steelblue');

    svg.append('g')
      .attr('transform', `translate(0,${height - 80})`)
      .call(d3.axisBottom(x))
      .selectAll('text')
      .attr('transform', 'rotate(-45)')
      .style('text-anchor', 'end');

    svg.append('g')
      .attr('transform', 'translate(80,0)')
      .call(d3.axisLeft(y));
  }
}
