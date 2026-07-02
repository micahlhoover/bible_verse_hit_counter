import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';

@Injectable({
  providedIn: 'root',
})
export class BibleApi {
    private apiUrl =
    "https://bible-verse-hit-counter-1.onrender.com/books";
    //'https://your-render-api.onrender.com/api/hits';

  constructor(private http: HttpClient) {}

  getHits() {
    return this.http.get<any>(this.apiUrl);
  }
}
