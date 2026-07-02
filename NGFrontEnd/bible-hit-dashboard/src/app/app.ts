import { Component, signal, OnInit } from '@angular/core';
import { RouterOutlet } from '@angular/router';
import { BibleApi } from './services/bible-api';

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
    this.bibleApi.getHits().subscribe(data => {
      console.log(data);
    });
  }
}
