import React from "react";
import ReactDOM from "react-dom/client";
import { BrowserRouter, Route, Routes } from "react-router-dom";
import Dashboard from "./pages";
import ALSApacheSpark from "./pages/als-apache-spark";
import ALSImplicit from "./pages/als-implicit";
import BPRImplicit from "./pages/bpr-implicit";
import Straightforward from "./pages/straightforward";
import "./scss/main.scss";

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <BrowserRouter>
      <Routes>
        <Route path='/' element={<Dashboard />} />
        <Route path='/straightforward' element={<Straightforward />} />
        <Route path='/als-implicit' element={<ALSImplicit />} />
        <Route path='/als-apache' element={<ALSApacheSpark />} />
        <Route path='/bpr-implicit' element={<BPRImplicit />} />
      </Routes>
    </BrowserRouter>
  </React.StrictMode>
);
